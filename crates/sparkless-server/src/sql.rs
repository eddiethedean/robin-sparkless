use chrono::{DateTime, Duration, NaiveDate, Utc};
use robin_sparkless::{
    AnyValue, CancellationToken, PlDataType, SparkSession, TimeUnit,
    sql::{ast, execute_query_ast_with_cancellation_root},
};
use serde_json::{Map, Number, Value, json};
use spark_sql_parser::{SparkStatement, ast::Statement};
use std::collections::HashMap;

use crate::config::Limits;

const INTERNAL_ALIAS_PREFIX: &str = "__sparkless_416_internal_";

#[derive(Clone, Debug)]
pub struct SqlError {
    pub name: &'static str,
    pub message: String,
}

impl SqlError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            name: "InvalidSql",
            message: message.into(),
        }
    }

    fn unsupported(message: impl Into<String>) -> Self {
        Self {
            name: "UnsupportedOperation",
            message: message.into(),
        }
    }

    fn result_limit(message: impl Into<String>) -> Self {
        Self {
            name: "ResultLimitExceeded",
            message: message.into(),
        }
    }

    fn execution(error: impl std::fmt::Display) -> Self {
        Self {
            name: "QueryExecutionError",
            message: error.to_string(),
        }
    }
}

#[derive(Clone, Debug)]
struct ColumnMeta {
    name: String,
    nullable: bool,
}

pub struct ValidatedQuery {
    query: ast::Query,
    public_names: Vec<String>,
    nullable: Vec<bool>,
}

pub struct QueryResult {
    pub schema: Value,
    pub rows: Vec<Vec<Value>>,
}

/// Parse, validate and rewrite only duplicate top-level projection labels.
///
/// The returned query AST is the exact AST passed to native execution. Rewrites
/// change private output names only; `public_names` restores client labels by
/// ordinal after evaluation.
pub fn validate_query(sql: &str, limits: &Limits) -> Result<ValidatedQuery, SqlError> {
    preflight_sql(sql, limits.max_sql_depth, limits.max_sql_nodes)?;
    let parsed = spark_sql_parser::parse_spark_sql_with_recursion_limit(sql, limits.max_sql_depth)
        .map_err(|error| SqlError::invalid(error.to_string()))?;
    let query = match parsed {
        SparkStatement::Sqlparser(statement) => match *statement {
            Statement::Query(query) => *query,
            _ => {
                return Err(SqlError::unsupported(
                    "only read-only SELECT queries are available through this endpoint",
                ));
            }
        },
        _ => {
            return Err(SqlError::unsupported(
                "only read-only SELECT queries are available through this endpoint",
            ));
        }
    };
    let mut query = query;
    let mut budget = AstBudget {
        remaining: limits.max_sql_nodes,
        max_columns: limits.max_columns,
        max_depth: limits.max_sql_depth,
    };
    let output = validate_query_node(&query, &mut budget, 0, true)?;
    let public_names = output
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let nullable = output.iter().map(|column| column.nullable).collect();
    rewrite_duplicate_top_level_names(&mut query, &output)?;
    Ok(ValidatedQuery {
        query,
        public_names,
        nullable,
    })
}

pub fn execute_query(
    session: &SparkSession,
    query: ValidatedQuery,
    token: &CancellationToken,
    limits: &Limits,
) -> Result<QueryResult, SqlError> {
    let dataframe = execute_query_ast_with_cancellation_root(session, &query.query, token)
        .map_err(map_engine_error)?;
    token.check_cancelled().map_err(map_engine_error)?;
    let physical_names = dataframe.columns_engine().map_err(map_engine_error)?;
    if physical_names.len() != query.nullable.len() {
        return Err(SqlError::execution(format!(
            "native engine returned {} columns for a {} column query",
            physical_names.len(),
            query.nullable.len()
        )));
    }
    if physical_names.len() > limits.max_columns {
        return Err(SqlError::result_limit(
            "query exceeds the output column limit",
        ));
    }

    let mut fields = Vec::with_capacity(physical_names.len());
    for (index, physical_name) in physical_names.iter().enumerate() {
        let physical_type = dataframe.get_column_dtype(physical_name).ok_or_else(|| {
            SqlError::execution(format!("native schema is missing column {index}"))
        })?;
        let type_name = spark_type_name(&physical_type)?;
        let name = query.public_names[index].clone();
        let mut field = Map::new();
        field.insert("name".into(), Value::String(name));
        field.insert("type".into(), Value::String(type_name));
        field.insert("nullable".into(), Value::Bool(query.nullable[index]));
        field.insert("metadata".into(), json!({}));
        fields.push(Value::Object(field));
    }
    let schema = json!({"type": "struct", "fields": fields});

    // Pull one row at a time. On cancellation or a result limit, keep draining
    // the native iterator so its detached producer has exited before the
    // caller releases the compute permit.
    let batches = dataframe.collect_batches(1).map_err(SqlError::execution)?;
    let mut rows = Vec::new();
    let empty_envelope = crate::protocol::success_envelope(schema.clone(), Vec::new());
    let mut encoded_size = serde_json::to_vec(&empty_envelope)
        .map_err(SqlError::execution)?
        .len();
    let mut failure = None;
    for batch in batches {
        if failure.is_none() && token.is_cancelled() {
            failure = Some(map_engine_error("operation cancelled"));
        }
        let batch = match batch {
            Ok(batch) if failure.is_none() => batch,
            Ok(_) => continue,
            Err(error) => {
                failure.get_or_insert_with(|| SqlError::execution(error));
                continue;
            }
        };
        let batch_result = (|| {
            for row_index in 0..batch.height() {
                if rows.len() >= limits.max_rows {
                    return Err(SqlError::result_limit("query exceeds the output row limit"));
                }
                let mut row = Vec::with_capacity(physical_names.len());
                // Each row is a JSON array, including its brackets for empty schemas.
                let mut row_content_bytes = 2usize;
                for (column_index, column) in batch.columns().iter().enumerate() {
                    let cell = encode_cell(column.get(row_index).map_err(SqlError::execution)?)?;
                    let cell_size = serde_json::to_vec(&cell)
                        .map_err(SqlError::execution)?
                        .len();
                    if cell_size > limits.max_cell_bytes {
                        return Err(SqlError::result_limit(format!(
                            "encoded cell at row {}, column {} exceeds the single-cell limit",
                            rows.len(),
                            column_index
                        )));
                    }
                    row_content_bytes = row_content_bytes.saturating_add(cell_size);
                    if column_index > 0 {
                        row_content_bytes = row_content_bytes.saturating_add(1);
                    }
                    let row_separator = usize::from(!rows.is_empty());
                    let replaced_empty_array = usize::from(rows.is_empty()) * 2;
                    if encoded_size
                        .saturating_add(row_separator)
                        .saturating_add(row_content_bytes)
                        .saturating_sub(replaced_empty_array)
                        > limits.max_response_bytes
                    {
                        return Err(SqlError::result_limit(
                            "encoded result exceeds the complete response byte limit",
                        ));
                    }
                    row.push(cell);
                }
                let replaced_empty_array = usize::from(rows.is_empty()) * 2;
                encoded_size = encoded_size
                    .saturating_add(usize::from(!rows.is_empty()))
                    .saturating_add(row_content_bytes)
                    .saturating_sub(replaced_empty_array);
                if encoded_size > limits.max_response_bytes {
                    return Err(SqlError::result_limit(
                        "encoded result exceeds the complete response byte limit",
                    ));
                }
                rows.push(row);
            }
            Ok(())
        })();
        if let Err(error) = batch_result {
            failure = Some(error);
        }
    }
    if let Some(error) = failure {
        return Err(error);
    }
    token.check_cancelled().map_err(map_engine_error)?;
    Ok(QueryResult { schema, rows })
}

fn map_engine_error(error: impl std::fmt::Display) -> SqlError {
    let message = error.to_string();
    if message.to_ascii_lowercase().contains("cancelled") {
        SqlError {
            name: "Cancelled",
            message,
        }
    } else {
        SqlError::execution(message)
    }
}

#[derive(Clone)]
struct SourceBinding {
    alias: Option<String>,
    columns: Vec<ColumnMeta>,
}

struct AstBudget {
    remaining: usize,
    max_columns: usize,
    max_depth: usize,
}

impl AstBudget {
    fn take(&mut self) -> Result<(), SqlError> {
        self.remaining = self
            .remaining
            .checked_sub(1)
            .ok_or_else(|| SqlError::invalid("SQL query exceeds the AST node limit"))?;
        Ok(())
    }
}

fn validate_query_node(
    query: &ast::Query,
    budget: &mut AstBudget,
    depth: usize,
    allow_duplicate_names: bool,
) -> Result<Vec<ColumnMeta>, SqlError> {
    budget.take()?;
    if depth > budget.max_depth || query.with.is_some() {
        return Err(SqlError::unsupported(
            "CTEs or SQL nesting beyond the configured depth are not supported",
        ));
    }
    if query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(SqlError::unsupported("unsupported SELECT query clause"));
    }
    if let Some(limit_clause) = &query.limit_clause {
        match limit_clause {
            ast::LimitClause::LimitOffset {
                limit: Some(limit),
                offset: None,
                limit_by,
            } if limit_by.is_empty() => {
                validate_limit(limit)?;
                budget.take()?;
            }
            ast::LimitClause::LimitOffset { limit: None, .. } => {
                return Err(SqlError::unsupported("LIMIT ALL is not supported"));
            }
            _ => {
                return Err(SqlError::unsupported(
                    "only a nonnegative literal LIMIT is supported",
                ));
            }
        }
    }
    let mut output = validate_set_expr(&query.body, budget, depth + 1, allow_duplicate_names)?;
    if output.len() > budget.max_columns {
        return Err(SqlError::result_limit(
            "query exceeds the output column limit",
        ));
    }
    if let Some(order_by) = &query.order_by {
        if order_by.interpolate.is_some() {
            return Err(SqlError::unsupported(
                "ORDER BY interpolation is not supported",
            ));
        }
        let ast::OrderByKind::Expressions(expressions) = &order_by.kind else {
            return Err(SqlError::unsupported(
                "only column-based ORDER BY is supported",
            ));
        };
        for ordering in expressions {
            budget.take()?;
            validate_order_expression(&ordering.expr, &output, budget)?;
            if ordering.with_fill.is_some() {
                return Err(SqlError::unsupported("ORDER BY fill is not supported"));
            }
        }
    }
    // Query output order is positional. This field is modified only by a
    // private-alias rewrite after validation.
    Ok(std::mem::take(&mut output))
}

fn validate_set_expr(
    set_expr: &ast::SetExpr,
    budget: &mut AstBudget,
    depth: usize,
    allow_duplicate_names: bool,
) -> Result<Vec<ColumnMeta>, SqlError> {
    budget.take()?;
    if depth > budget.max_depth {
        return Err(SqlError::unsupported(
            "CTEs or SQL nesting beyond the configured depth are not supported",
        ));
    }
    match set_expr {
        ast::SetExpr::Select(select) => {
            validate_select(select, budget, depth + 1, allow_duplicate_names)
        }
        ast::SetExpr::Query(query) => validate_query_node(query, budget, depth + 1, false),
        ast::SetExpr::SetOperation {
            op: ast::SetOperator::Union,
            set_quantifier: ast::SetQuantifier::All,
            left,
            right,
        } => {
            let left = validate_set_expr(left, budget, depth + 1, false)?;
            let right = validate_set_expr(right, budget, depth + 1, false)?;
            if left.len() != right.len() {
                return Err(SqlError::invalid(
                    "UNION ALL branches must return the same number of columns",
                ));
            }
            Ok(left
                .into_iter()
                .zip(right)
                .map(|(mut l, r)| {
                    l.nullable |= r.nullable;
                    l
                })
                .collect())
        }
        ast::SetExpr::SetOperation { .. } => Err(SqlError::unsupported(
            "only UNION ALL set operations are supported",
        )),
        _ => Err(SqlError::unsupported(
            "only SELECT queries and bounded UNION ALL are supported",
        )),
    }
}

fn validate_select(
    select: &ast::Select,
    budget: &mut AstBudget,
    depth: usize,
    allow_duplicate_names: bool,
) -> Result<Vec<ColumnMeta>, SqlError> {
    budget.take()?;
    if depth > budget.max_depth
        || select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !matches!(&select.group_by, ast::GroupByExpr::Expressions(exprs, mods) if exprs.is_empty() && mods.is_empty())
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || !matches!(select.flavor, ast::SelectFlavor::Standard)
    {
        return Err(SqlError::unsupported(
            "SELECT supports projections, derived constants, WHERE, ORDER BY and LIMIT only",
        ));
    }
    if select.projection.is_empty() || select.projection.len() > budget.max_columns {
        return Err(SqlError::result_limit(
            "query exceeds the output column limit",
        ));
    }
    if select.from.len() > 1 {
        return Err(SqlError::unsupported(
            "joins and comma relations are not supported",
        ));
    }
    let mut bindings = Vec::new();
    if let Some(from) = select.from.first() {
        budget.take()?;
        if !from.joins.is_empty() {
            return Err(SqlError::unsupported("joins are not supported"));
        }
        match &from.relation {
            ast::TableFactor::Derived {
                lateral: false,
                subquery,
                alias,
                sample: None,
            } => {
                if alias
                    .as_ref()
                    .is_some_and(|alias| !alias.columns.is_empty())
                {
                    return Err(SqlError::unsupported(
                        "derived-table column renaming is not supported",
                    ));
                }
                let columns = validate_query_node(subquery, budget, depth + 1, false)?;
                bindings.push(SourceBinding {
                    alias: alias.as_ref().map(|a| a.name.value.clone()),
                    columns,
                });
            }
            _ => {
                return Err(SqlError::unsupported(
                    "only parenthesized constant SELECT relations are supported; named tables are disabled",
                ));
            }
        }
    }
    if let Some(selection) = &select.selection {
        validate_expr(selection, &bindings, budget, depth + 1)?;
    }
    let mut columns = Vec::with_capacity(select.projection.len());
    for projection in &select.projection {
        let (expr, alias) = match projection {
            ast::SelectItem::UnnamedExpr(expr) => (expr, None),
            ast::SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            _ => {
                return Err(SqlError::unsupported(
                    "wildcard projections are not supported",
                ));
            }
        };
        let nullable = validate_expr(expr, &bindings, budget, depth + 1)?;
        let output_name = alias.clone().unwrap_or_else(|| output_name_for_expr(expr));
        columns.push(ColumnMeta {
            name: output_name,
            nullable,
        });
    }
    reject_duplicate_names(&columns, allow_duplicate_names)?;
    Ok(columns)
}

fn validate_expr(
    expr: &ast::Expr,
    bindings: &[SourceBinding],
    budget: &mut AstBudget,
    depth: usize,
) -> Result<bool, SqlError> {
    budget.take()?;
    if depth > budget.max_depth {
        return Err(SqlError::unsupported(
            "expression nesting exceeds the configured depth",
        ));
    }
    match expr {
        ast::Expr::Value(value) => match &value.value {
            ast::Value::Number(_, _)
            | ast::Value::SingleQuotedString(_)
            | ast::Value::Boolean(_) => Ok(false),
            ast::Value::Null => Err(SqlError::unsupported(
                "bare untyped NULL is not supported; cast NULL to a scalar type",
            )),
            _ => Err(SqlError::unsupported(
                "only numeric, string, boolean and typed NULL literals are supported",
            )),
        },
        ast::Expr::TypedString(value) => {
            let type_name = value.data_type.to_string().to_ascii_lowercase();
            if !matches!(type_name.as_str(), "date" | "timestamp")
                || !matches!(value.value.value, ast::Value::SingleQuotedString(_))
            {
                return Err(SqlError::unsupported(
                    "only DATE and TIMESTAMP typed string literals are supported",
                ));
            }
            Ok(false)
        }
        ast::Expr::Identifier(identifier) => lookup_column(bindings, None, &identifier.value),
        ast::Expr::CompoundIdentifier(identifiers) if identifiers.len() == 2 => {
            let qualifier = identifiers[identifiers.len() - 2].value.as_str();
            let column = identifiers
                .last()
                .expect("nonempty identifier")
                .value
                .as_str();
            lookup_column(bindings, Some(qualifier), column)
        }
        ast::Expr::UnaryOp { op, expr } => {
            if !matches!(
                op,
                ast::UnaryOperator::Plus | ast::UnaryOperator::Minus | ast::UnaryOperator::Not
            ) {
                return Err(SqlError::unsupported("unary operator is not supported"));
            }
            validate_expr(expr, bindings, budget, depth + 1)
        }
        ast::Expr::BinaryOp { left, op, right } => {
            let operator = op.to_string().to_ascii_uppercase();
            if !matches!(
                operator.as_str(),
                "+" | "-"
                    | "*"
                    | "/"
                    | "%"
                    | "="
                    | "!="
                    | "<>"
                    | "<"
                    | "<="
                    | ">"
                    | ">="
                    | "AND"
                    | "OR"
            ) {
                return Err(SqlError::unsupported("binary operator is not supported"));
            }
            let left_null = validate_expr(left, bindings, budget, depth + 1)?;
            let right_null = validate_expr(right, bindings, budget, depth + 1)?;
            Ok(left_null || right_null)
        }
        ast::Expr::Nested(inner) => validate_expr(inner, bindings, budget, depth + 1),
        ast::Expr::IsNull(inner) | ast::Expr::IsNotNull(inner) => {
            validate_expr(inner, bindings, budget, depth + 1)?;
            Ok(false)
        }
        ast::Expr::Cast {
            kind: ast::CastKind::Cast,
            expr,
            data_type,
            array: false,
            format: None,
        } => {
            validate_cast_type(data_type)?;
            if is_untyped_null(expr) {
                Ok(true)
            } else {
                validate_expr(expr, bindings, budget, depth + 1)
            }
        }
        _ => Err(SqlError::unsupported(
            "functions, subqueries, predicates and expression forms outside the scalar allowlist are disabled",
        )),
    }
}

fn validate_cast_type(data_type: &ast::DataType) -> Result<(), SqlError> {
    let compact = data_type
        .to_string()
        .to_ascii_lowercase()
        .replace(' ', "")
        .replace("numeric", "decimal");
    let supported = matches!(
        compact.as_str(),
        "boolean"
            | "bool"
            | "smallint"
            | "int2"
            | "int"
            | "integer"
            | "int4"
            | "bigint"
            | "int8"
            | "double"
            | "doubleprecision"
            | "string"
            | "varchar"
            | "date"
            | "timestamp"
            | "timestampwithouttimezone"
    ) || parse_decimal_type(&compact).is_some();
    if supported {
        Ok(())
    } else {
        Err(SqlError::unsupported(format!(
            "CAST to {data_type} is outside the supported scalar type set"
        )))
    }
}

fn is_untyped_null(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Value(value) => matches!(&value.value, ast::Value::Null),
        ast::Expr::Nested(inner) => is_untyped_null(inner),
        _ => false,
    }
}

fn parse_decimal_type(value: &str) -> Option<(usize, usize)> {
    let body = value.strip_prefix("decimal(")?.strip_suffix(')')?;
    let (precision, scale) = body.split_once(',')?;
    let precision = precision.parse::<usize>().ok()?;
    let scale = scale.parse::<usize>().ok()?;
    (1..=38).contains(&precision).then_some(())?;
    (scale <= precision).then_some((precision, scale))
}

fn validate_limit(expr: &ast::Expr) -> Result<(), SqlError> {
    match expr {
        ast::Expr::Value(value) => match &value.value {
            ast::Value::Number(number, _) if number.parse::<u64>().is_ok() => Ok(()),
            _ => Err(SqlError::unsupported(
                "LIMIT must be a nonnegative integer literal",
            )),
        },
        _ => Err(SqlError::unsupported(
            "LIMIT must be a nonnegative integer literal",
        )),
    }
}

fn validate_order_expression(
    expr: &ast::Expr,
    output: &[ColumnMeta],
    budget: &mut AstBudget,
) -> Result<(), SqlError> {
    budget.take()?;
    let name = match expr {
        ast::Expr::Identifier(identifier) => identifier.value.as_str(),
        ast::Expr::CompoundIdentifier(parts) if parts.len() == 1 => parts[0].value.as_str(),
        _ => {
            return Err(SqlError::unsupported(
                "ORDER BY accepts output column names only",
            ));
        }
    };
    if output
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(name))
        .count()
        != 1
    {
        return Err(SqlError::invalid(format!(
            "ORDER BY column '{name}' is unknown or ambiguous"
        )));
    }
    Ok(())
}

fn lookup_column(
    bindings: &[SourceBinding],
    qualifier: Option<&str>,
    name: &str,
) -> Result<bool, SqlError> {
    let mut matches = Vec::new();
    for binding in bindings {
        if let Some(qualifier) = qualifier
            && !binding
                .alias
                .as_deref()
                .is_some_and(|alias| alias.eq_ignore_ascii_case(qualifier))
        {
            continue;
        }
        matches.extend(
            binding
                .columns
                .iter()
                .filter(|column| column.name.eq_ignore_ascii_case(name)),
        );
    }
    match matches.as_slice() {
        [column] => Ok(column.nullable),
        [] => Err(SqlError::invalid(format!(
            "column '{name}' is not available in this query"
        ))),
        _ => Err(SqlError::invalid(format!("column '{name}' is ambiguous"))),
    }
}

fn output_name_for_expr(expr: &ast::Expr) -> String {
    match expr {
        ast::Expr::Identifier(identifier) => identifier.value.clone(),
        ast::Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|part| part.value.clone())
            .unwrap_or_default(),
        _ => expr.to_string(),
    }
}

fn reject_duplicate_names(columns: &[ColumnMeta], allow_duplicates: bool) -> Result<(), SqlError> {
    let mut seen = HashMap::<String, usize>::new();
    let duplicate = columns
        .iter()
        .find(|column| seen.insert(column.name.to_ascii_lowercase(), 1).is_some());
    if duplicate.is_some() && !allow_duplicates {
        Err(SqlError::unsupported(
            "duplicate labels are supported only on a top-level SELECT projection",
        ))
    } else {
        Ok(())
    }
}

fn rewrite_duplicate_top_level_names(
    query: &mut ast::Query,
    output: &[ColumnMeta],
) -> Result<(), SqlError> {
    let ast::SetExpr::Select(select) = query.body.as_mut() else {
        if has_duplicate_names(output) {
            return Err(SqlError::unsupported(
                "duplicate labels in UNION ALL or parenthesized queries are not supported",
            ));
        }
        return Ok(());
    };
    if !has_duplicate_names(output) {
        return Ok(());
    }
    let mut counts = HashMap::<String, usize>::new();
    for column in output {
        *counts.entry(column.name.to_ascii_lowercase()).or_default() += 1;
    }
    let mut taken = output
        .iter()
        .map(|column| column.name.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    for (index, projection) in select.projection.iter_mut().enumerate() {
        let name = &output[index].name;
        if counts
            .get(&name.to_ascii_lowercase())
            .copied()
            .unwrap_or_default()
            < 2
        {
            continue;
        }
        let mut internal = format!("{INTERNAL_ALIAS_PREFIX}{index}");
        let mut suffix = 0usize;
        while taken.contains(&internal.to_ascii_lowercase()) {
            suffix += 1;
            internal = format!("{INTERNAL_ALIAS_PREFIX}{index}_{suffix}");
        }
        taken.insert(internal.to_ascii_lowercase());
        let (expr, _) = match projection {
            ast::SelectItem::UnnamedExpr(expr) => (expr.clone(), None),
            ast::SelectItem::ExprWithAlias { expr, alias } => (expr.clone(), Some(alias.clone())),
            _ => {
                return Err(SqlError::unsupported(
                    "wildcard projections are not supported",
                ));
            }
        };
        *projection = ast::SelectItem::ExprWithAlias {
            expr,
            alias: ast::Ident::new(internal),
        };
    }
    if query.order_by.as_ref().is_some_and(|order| {
        matches!(&order.kind, ast::OrderByKind::Expressions(expressions) if expressions.iter().any(|item| {
            matches!(&item.expr, ast::Expr::Identifier(identifier) if counts.get(&identifier.value.to_ascii_lowercase()).copied().unwrap_or_default() > 1)
        }))
    }) {
        return Err(SqlError::invalid(
            "ORDER BY cannot refer to a duplicate output label",
        ));
    }
    Ok(())
}

fn has_duplicate_names(output: &[ColumnMeta]) -> bool {
    let mut seen = std::collections::HashSet::new();
    output
        .iter()
        .any(|column| !seen.insert(column.name.to_ascii_lowercase()))
}

fn preflight_sql(sql: &str, max_depth: usize, max_nodes: usize) -> Result<(), SqlError> {
    #[derive(Clone, Copy)]
    enum Mode {
        Plain,
        Single,
        Double,
        Backtick,
        LineComment,
        BlockComment,
    }
    let bytes = sql.as_bytes();
    let mut mode = Mode::Plain;
    let mut depth = 0usize;
    let mut nodes = 0usize;
    let mut token_len = 0usize;
    let mut token_start = 0usize;
    let mut union_count = 0usize;
    let mut literal_len = 0usize;
    let mut index = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        let next = bytes.get(index + 1).copied();
        match mode {
            Mode::LineComment => {
                if byte == b'\n' {
                    mode = Mode::Plain;
                }
            }
            Mode::BlockComment => {
                if byte == b'*' && next == Some(b'/') {
                    mode = Mode::Plain;
                    index += 1;
                }
            }
            Mode::Single | Mode::Double | Mode::Backtick => {
                let closing = match mode {
                    Mode::Single => b'\'',
                    Mode::Double => b'"',
                    Mode::Backtick => b'`',
                    _ => unreachable!(),
                };
                if byte == closing {
                    if next == Some(closing) {
                        index += 1;
                        if !matches!(mode, Mode::Backtick) {
                            literal_len += 1;
                        }
                    } else {
                        if matches!(mode, Mode::Backtick) && token_len > 256 {
                            return Err(SqlError::invalid("SQL identifier exceeds 256 bytes"));
                        }
                        if !matches!(mode, Mode::Backtick) && literal_len > 64 * 1024 {
                            return Err(SqlError::invalid("SQL string literal exceeds 64 KiB"));
                        }
                        mode = Mode::Plain;
                        token_len = 0;
                        literal_len = 0;
                    }
                } else if matches!(mode, Mode::Backtick) {
                    token_len += 1;
                } else {
                    literal_len += 1;
                }
            }
            Mode::Plain => {
                if token_len > 0
                    && !(byte.is_ascii_alphanumeric() || byte == b'_' || byte >= 0x80)
                    && sql[token_start..index].eq_ignore_ascii_case("union")
                {
                    union_count += 1;
                    if union_count > max_depth {
                        return Err(SqlError::invalid(
                            "SQL UNION chain exceeds the configured depth limit",
                        ));
                    }
                }
                if byte == b'-' && next == Some(b'-') {
                    mode = Mode::LineComment;
                    index += 1;
                } else if byte == b'/' && next == Some(b'*') {
                    mode = Mode::BlockComment;
                    index += 1;
                } else if byte == b'\'' {
                    mode = Mode::Single;
                    literal_len = 0;
                    nodes += 1;
                } else if byte == b'"' {
                    mode = Mode::Double;
                    literal_len = 0;
                    nodes += 1;
                } else if byte == b'`' {
                    mode = Mode::Backtick;
                    token_len = 0;
                    nodes += 1;
                } else if byte == b'(' {
                    depth += 1;
                    nodes += 1;
                    if depth > max_depth {
                        return Err(SqlError::invalid(
                            "SQL nesting exceeds the configured depth limit",
                        ));
                    }
                } else if byte == b')' {
                    depth = depth.saturating_sub(1);
                    nodes += 1;
                } else if byte.is_ascii_whitespace() {
                    token_len = 0;
                } else if byte.is_ascii_alphanumeric() || byte == b'_' || byte >= 0x80 {
                    if token_len == 0 {
                        token_start = index;
                    }
                    token_len += 1;
                    if token_len == 1 {
                        nodes += 1;
                    }
                    if token_len > 256 {
                        return Err(SqlError::invalid("SQL identifier exceeds 256 bytes"));
                    }
                } else {
                    token_len = 0;
                    nodes += 1;
                }
                if nodes > max_nodes {
                    return Err(SqlError::invalid(
                        "SQL query exceeds the configured AST node limit",
                    ));
                }
            }
        }
        index += 1;
    }
    Ok(())
}

fn spark_type_name(dtype: &PlDataType) -> Result<String, SqlError> {
    match dtype {
        PlDataType::Boolean => Ok("boolean".into()),
        PlDataType::Int16 => Ok("short".into()),
        PlDataType::Int32 => Ok("integer".into()),
        PlDataType::Int64 => Ok("long".into()),
        PlDataType::Float64 => Ok("double".into()),
        PlDataType::String => Ok("string".into()),
        PlDataType::Date => Ok("date".into()),
        PlDataType::Datetime(TimeUnit::Microseconds, timezone) => {
            if timezone
                .as_ref()
                .is_some_and(|timezone| !timezone.as_str().eq_ignore_ascii_case("UTC"))
            {
                return Err(SqlError::unsupported(
                    "only fixed UTC timestamp results are supported",
                ));
            }
            Ok("timestamp".into())
        }
        PlDataType::Datetime(_, _) => Err(SqlError::unsupported(
            "only microsecond-precision timestamps are supported",
        )),
        PlDataType::Decimal(precision, scale)
            if (1..=38).contains(precision) && scale <= precision =>
        {
            Ok(format!("decimal({precision},{scale})"))
        }
        _ => Err(SqlError::unsupported(format!(
            "native result type {dtype:?} is outside the supported scalar type set"
        ))),
    }
}

fn encode_cell(value: AnyValue<'_>) -> Result<Value, SqlError> {
    match value {
        AnyValue::Null => Ok(Value::Null),
        AnyValue::Boolean(value) => Ok(Value::Bool(value)),
        AnyValue::Int16(value) => Ok(json!(value)),
        AnyValue::Int32(value) => Ok(json!(value)),
        AnyValue::Int64(value) => Ok(json!(value)),
        AnyValue::Float64(value) if value.is_finite() => Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| SqlError::unsupported("non-finite DOUBLE values are not supported")),
        AnyValue::Float64(_) => Err(SqlError::unsupported(
            "non-finite DOUBLE values are not supported",
        )),
        AnyValue::String(value) => Ok(Value::String(value.to_owned())),
        AnyValue::StringOwned(value) => Ok(Value::String(value.to_string())),
        AnyValue::Date(days) => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
            let date = epoch
                .checked_add_signed(Duration::days(i64::from(days)))
                .ok_or_else(|| {
                    SqlError::unsupported("DATE value is outside the supported range")
                })?;
            Ok(Value::String(date.format("%Y-%m-%d").to_string()))
        }
        AnyValue::Datetime(value, unit, timezone) => {
            encode_timestamp(value, unit, timezone.map(|zone| zone.as_str()))
        }
        AnyValue::DatetimeOwned(value, unit, timezone) => {
            encode_timestamp(value, unit, timezone.as_deref().map(|zone| zone.as_str()))
        }
        AnyValue::Decimal(value, precision, scale) => {
            if precision == 0 || precision > 38 || scale > precision {
                return Err(SqlError::unsupported(
                    "native DECIMAL precision or scale is unsupported",
                ));
            }
            Ok(Value::String(format_decimal(value, scale)))
        }
        _ => Err(SqlError::unsupported(
            "native result cell type is outside the supported scalar set",
        )),
    }
}

fn encode_timestamp(value: i64, unit: TimeUnit, timezone: Option<&str>) -> Result<Value, SqlError> {
    if timezone.is_some_and(|zone| !zone.eq_ignore_ascii_case("UTC")) {
        return Err(SqlError::unsupported(
            "only UTC timestamp values are supported",
        ));
    }
    let micros = match unit {
        TimeUnit::Nanoseconds if value % 1_000 == 0 => value / 1_000,
        TimeUnit::Nanoseconds => {
            return Err(SqlError::unsupported(
                "sub-microsecond timestamp precision is not supported",
            ));
        }
        TimeUnit::Microseconds => value,
        TimeUnit::Milliseconds => value
            .checked_mul(1_000)
            .ok_or_else(|| SqlError::unsupported("TIMESTAMP is outside the supported range"))?,
    };
    let datetime = DateTime::<Utc>::from_timestamp_micros(micros)
        .ok_or_else(|| SqlError::unsupported("TIMESTAMP is outside the supported range"))?;
    Ok(Value::String(
        datetime.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string(),
    ))
}

fn format_decimal(value: i128, scale: usize) -> String {
    let negative = value < 0;
    let digits = value.unsigned_abs().to_string();
    let mut result = if scale == 0 {
        digits
    } else if digits.len() <= scale {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    } else {
        let point = digits.len() - scale;
        format!("{}.{}", &digits[..point], &digits[point..])
    };
    if negative {
        result.insert(0, '-');
    }
    result
}

#[cfg(test)]
mod tests {
    use super::preflight_sql;

    #[test]
    fn rejects_a_long_union_chain_before_building_a_recursive_ast() {
        let sql = std::iter::repeat_n("SELECT 1", 10_000)
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        let error = preflight_sql(&sql, 64, 100_000).unwrap_err();
        assert_eq!(error.name, "InvalidSql");
        assert!(error.message.contains("UNION chain"));
    }

    #[test]
    fn does_not_count_union_words_inside_literals_or_comments() {
        preflight_sql(
            "SELECT 'UNION ALL' AS label /* UNION ALL */ -- UNION ALL\n",
            64,
            100,
        )
        .unwrap();
    }
}
