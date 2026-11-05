//! Engine-neutral relational structure lowered from the parser AST.
//!
//! Expressions retain their normalized SQL spelling and a small structural summary. This keeps
//! the contract independent of `sqlparser` while letting adapters own expression semantics.

use crate::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, JoinConstraint,
    JoinOperator, LimitClause, OrderByKind, Query, SelectItem, SetExpr,
    SetQuantifier as AstSetQuantifier, TableFactor, Value, ValueWithSpan, VisitMut, VisitorMut,
};
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, ops::ControlFlow};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlQueryPlan {
    pub source_sql: String,
    pub recursive: bool,
    pub ctes: Vec<CtePlan>,
    pub body: RelationalNode,
    pub order_by: Vec<SortKey>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CtePlan {
    pub name: String,
    pub columns: Vec<String>,
    pub query: Box<SqlQueryPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelationalNode {
    Select(Box<SelectPlan>),
    SetOperation {
        operator: SetOperator,
        quantifier: SetQuantifier,
        left: Box<RelationalNode>,
        right: Box<RelationalNode>,
    },
    Values {
        rows: Vec<Vec<SqlExpression>>,
    },
    Nested(Box<SqlQueryPlan>),
    Opaque {
        kind: String,
        sql: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectPlan {
    pub source_sql: String,
    pub distinct: Option<String>,
    pub projection: Vec<Projection>,
    pub from: Vec<FromPlan>,
    pub filter: Option<SqlExpression>,
    pub group_by: GroupByPlan,
    pub having: Option<SqlExpression>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupByPlan {
    Expressions {
        expressions: Vec<SqlExpression>,
        modifiers: Vec<String>,
    },
    All {
        modifiers: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SetOperator {
    Union,
    Except,
    Intersect,
    Minus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SetQuantifier {
    All,
    Distinct,
    ByName,
    AllByName,
    DistinctByName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Projection {
    Wildcard,
    QualifiedWildcard(String),
    Expression {
        expression: SqlExpression,
        alias: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FromPlan {
    pub relation: RelationPlan,
    pub joins: Vec<JoinPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelationPlan {
    Table {
        name: String,
        alias: Option<String>,
        source_sql: String,
    },
    Derived {
        query: Box<SqlQueryPlan>,
        alias: Option<String>,
        source_sql: String,
    },
    Opaque {
        kind: String,
        sql: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinPlan {
    pub relation: RelationPlan,
    pub global: bool,
    pub operator: JoinKind,
    pub constraint: JoinConstraintPlan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
    CrossApply,
    OuterApply,
    AsOf,
    Straight,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinConstraintPlan {
    On(SqlExpression),
    Using(Vec<String>),
    Natural,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortKey {
    pub expression: SqlExpression,
    pub ascending: Option<bool>,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlExpression {
    pub shape: ExpressionShape,
    pub sql: String,
    pub debug: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExpressionShape {
    Identifier(Vec<String>),
    Literal,
    Function {
        name: String,
        arguments: Vec<SqlExpression>,
        distinct: bool,
        has_star_argument: bool,
        has_unsupported_modifiers: bool,
    },
    Unary {
        operator: String,
        expression: Box<SqlExpression>,
    },
    Binary {
        operator: String,
        left: Box<SqlExpression>,
        right: Box<SqlExpression>,
    },
    Composite,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlPlanError(String);

impl std::fmt::Display for SqlPlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SqlPlanError {}

/// Lower a parsed query into backend-neutral relational nodes and clause records.
pub fn lower_query(query: &Query) -> Result<SqlQueryPlan, SqlPlanError> {
    let mut query = query.clone();
    let _ = VisitMut::visit(&mut query, &mut LoweredStrings::Encode);
    lower_canonical_query(&query)
}

pub(crate) enum LoweredStrings {
    Encode,
    Decode,
}

impl VisitorMut for LoweredStrings {
    type Break = Infallible;

    fn pre_visit_value(&mut self, value: &mut Value) -> ControlFlow<Self::Break> {
        let (text, constructor): (&mut String, fn(String) -> Value) = match (self, &mut *value) {
            // AST Display for ordinary quoted strings assumes some quotes are already
            // escaped. E literals explicitly escape every quote and backslash.
            (Self::Encode, Value::SingleQuotedString(text) | Value::DoubleQuotedString(text))
                if text.contains(['\'', '\\']) =>
            {
                (text, Value::EscapedStringLiteral)
            }
            (Self::Encode, Value::DoubleQuotedString(text))
            | (Self::Decode, Value::EscapedStringLiteral(text)) => {
                (text, Value::SingleQuotedString)
            }
            _ => return ControlFlow::Continue(()),
        };
        *value = constructor(std::mem::take(text));
        ControlFlow::Continue(())
    }
}

fn lower_canonical_query(query: &Query) -> Result<SqlQueryPlan, SqlPlanError> {
    let body = lower_set_expr(query.body.as_ref())?;
    let (recursive, ctes) = match &query.with {
        Some(with) => {
            let ctes = with
                .cte_tables
                .iter()
                .map(|cte| {
                    Ok(CtePlan {
                        name: cte.alias.name.value.clone(),
                        columns: cte
                            .alias
                            .columns
                            .iter()
                            .map(|column| column.name.value.clone())
                            .collect(),
                        query: Box::new(lower_canonical_query(&cte.query)?),
                    })
                })
                .collect::<Result<Vec<_>, SqlPlanError>>()?;
            (with.recursive, ctes)
        }
        None => (false, Vec::new()),
    };
    let mut order_by = Vec::new();
    if let Some(order) = &query.order_by
        && let OrderByKind::Expressions(expressions) = &order.kind
    {
        order_by.reserve(expressions.len());
        for item in expressions {
            order_by.push(SortKey {
                expression: lower_expression(&item.expr),
                ascending: item.options.asc,
                nulls_first: item.options.nulls_first,
            });
        }
    }

    let (limit, offset) = lower_limit(query)?;
    Ok(SqlQueryPlan {
        source_sql: query.to_string(),
        recursive,
        ctes,
        body,
        order_by,
        limit,
        offset,
    })
}

fn lower_set_expr(expr: &SetExpr) -> Result<RelationalNode, SqlPlanError> {
    Ok(match expr {
        SetExpr::Select(select) => {
            let projection = select
                .projection
                .iter()
                .map(|item| match item {
                    SelectItem::Wildcard(_) => Projection::Wildcard,
                    SelectItem::QualifiedWildcard(kind, _) => {
                        Projection::QualifiedWildcard(kind.to_string())
                    }
                    SelectItem::UnnamedExpr(expression) => Projection::Expression {
                        expression: lower_expression(expression),
                        alias: None,
                    },
                    SelectItem::ExprWithAlias { expr, alias } => Projection::Expression {
                        expression: lower_expression(expr),
                        alias: Some(alias.value.clone()),
                    },
                })
                .collect();
            let from = select
                .from
                .iter()
                .map(|item| {
                    let relation = lower_relation(&item.relation)?;
                    let joins = item
                        .joins
                        .iter()
                        .map(|join| {
                            Ok(JoinPlan {
                                relation: lower_relation(&join.relation)?,
                                global: join.global,
                                operator: join_kind(&join.join_operator),
                                constraint: join_constraint(&join.join_operator),
                            })
                        })
                        .collect::<Result<Vec<_>, SqlPlanError>>()?;
                    Ok(FromPlan { relation, joins })
                })
                .collect::<Result<Vec<_>, SqlPlanError>>()?;
            let group_by = match &select.group_by {
                GroupByExpr::Expressions(expressions, modifiers) => GroupByPlan::Expressions {
                    expressions: expressions.iter().map(lower_expression).collect(),
                    modifiers: modifiers.iter().map(ToString::to_string).collect(),
                },
                GroupByExpr::All(modifiers) => GroupByPlan::All {
                    modifiers: modifiers.iter().map(ToString::to_string).collect(),
                },
            };
            RelationalNode::Select(Box::new(SelectPlan {
                source_sql: select.to_string(),
                distinct: select.distinct.as_ref().map(ToString::to_string),
                projection,
                from,
                filter: select.selection.as_ref().map(lower_expression),
                group_by,
                having: select.having.as_ref().map(lower_expression),
            }))
        }
        SetExpr::Query(query) => RelationalNode::Nested(Box::new(lower_canonical_query(query)?)),
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => RelationalNode::SetOperation {
            operator: match op {
                crate::ast::SetOperator::Union => SetOperator::Union,
                crate::ast::SetOperator::Except => SetOperator::Except,
                crate::ast::SetOperator::Intersect => SetOperator::Intersect,
                crate::ast::SetOperator::Minus => SetOperator::Minus,
            },
            quantifier: match set_quantifier {
                AstSetQuantifier::All => SetQuantifier::All,
                AstSetQuantifier::Distinct | AstSetQuantifier::None => SetQuantifier::Distinct,
                AstSetQuantifier::ByName => SetQuantifier::ByName,
                AstSetQuantifier::AllByName => SetQuantifier::AllByName,
                AstSetQuantifier::DistinctByName => SetQuantifier::DistinctByName,
            },
            left: Box::new(lower_set_expr(left)?),
            right: Box::new(lower_set_expr(right)?),
        },
        SetExpr::Values(values) => RelationalNode::Values {
            rows: values
                .rows
                .iter()
                .map(|row| row.iter().map(lower_expression).collect())
                .collect(),
        },
        other => RelationalNode::Opaque {
            kind: "set_expression".into(),
            sql: other.to_string(),
        },
    })
}

fn join_kind(operator: &JoinOperator) -> JoinKind {
    match operator {
        JoinOperator::Join(_) | JoinOperator::Inner(_) => JoinKind::Inner,
        JoinOperator::Left(_) | JoinOperator::LeftOuter(_) => JoinKind::Left,
        JoinOperator::Right(_) | JoinOperator::RightOuter(_) => JoinKind::Right,
        JoinOperator::FullOuter(_) => JoinKind::Full,
        JoinOperator::CrossJoin(_) => JoinKind::Cross,
        JoinOperator::Semi(_) | JoinOperator::LeftSemi(_) => JoinKind::LeftSemi,
        JoinOperator::RightSemi(_) => JoinKind::RightSemi,
        JoinOperator::Anti(_) | JoinOperator::LeftAnti(_) => JoinKind::LeftAnti,
        JoinOperator::RightAnti(_) => JoinKind::RightAnti,
        JoinOperator::CrossApply => JoinKind::CrossApply,
        JoinOperator::OuterApply => JoinKind::OuterApply,
        JoinOperator::AsOf { .. } => JoinKind::AsOf,
        JoinOperator::StraightJoin(_) => JoinKind::Straight,
    }
}

fn join_constraint(operator: &JoinOperator) -> JoinConstraintPlan {
    let constraint = match operator {
        JoinOperator::Join(value)
        | JoinOperator::Inner(value)
        | JoinOperator::Left(value)
        | JoinOperator::LeftOuter(value)
        | JoinOperator::Right(value)
        | JoinOperator::RightOuter(value)
        | JoinOperator::FullOuter(value)
        | JoinOperator::CrossJoin(value)
        | JoinOperator::Semi(value)
        | JoinOperator::LeftSemi(value)
        | JoinOperator::RightSemi(value)
        | JoinOperator::Anti(value)
        | JoinOperator::LeftAnti(value)
        | JoinOperator::RightAnti(value)
        | JoinOperator::StraightJoin(value) => Some(value),
        JoinOperator::AsOf { constraint, .. } => Some(constraint),
        JoinOperator::CrossApply | JoinOperator::OuterApply => None,
    };
    match constraint {
        Some(JoinConstraint::On(expression)) => {
            JoinConstraintPlan::On(lower_expression(expression))
        }
        Some(JoinConstraint::Using(columns)) => {
            JoinConstraintPlan::Using(columns.iter().map(crate::object_name).collect())
        }
        Some(JoinConstraint::Natural) => JoinConstraintPlan::Natural,
        Some(JoinConstraint::None) | None => JoinConstraintPlan::None,
    }
}

fn lower_relation(factor: &TableFactor) -> Result<RelationPlan, SqlPlanError> {
    Ok(match factor {
        TableFactor::Table { name, alias, .. } => RelationPlan::Table {
            name: crate::object_name(name),
            alias: alias.as_ref().map(|alias| alias.name.value.clone()),
            source_sql: factor.to_string(),
        },
        TableFactor::Derived {
            subquery, alias, ..
        } => RelationPlan::Derived {
            query: Box::new(lower_canonical_query(subquery)?),
            alias: alias.as_ref().map(|alias| alias.name.value.clone()),
            source_sql: factor.to_string(),
        },
        other => RelationPlan::Opaque {
            kind: "table_factor".into(),
            sql: other.to_string(),
        },
    })
}

fn lower_expression(expr: &Expr) -> SqlExpression {
    let shape = match expr {
        Expr::Identifier(identifier) => ExpressionShape::Identifier(vec![identifier.value.clone()]),
        Expr::CompoundIdentifier(identifiers) => ExpressionShape::Identifier(
            identifiers
                .iter()
                .map(|identifier| identifier.value.clone())
                .collect(),
        ),
        Expr::Value(_) => ExpressionShape::Literal,
        Expr::Function(function) => {
            let mut distinct = false;
            let mut has_star_argument = false;
            let mut has_unsupported_modifiers = function.uses_odbc_syntax
                || !matches!(function.parameters, FunctionArguments::None)
                || function.filter.is_some()
                || function.null_treatment.is_some()
                || function.over.is_some()
                || !function.within_group.is_empty();
            let arguments = match &function.args {
                FunctionArguments::List(list) => {
                    distinct = list.duplicate_treatment.is_some();
                    has_unsupported_modifiers |= !list.clauses.is_empty();
                    list.args
                        .iter()
                        .filter_map(|argument| match argument {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                                Some(lower_expression(expr))
                            }
                            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                                has_star_argument = true;
                                None
                            }
                            FunctionArg::Named {
                                arg: FunctionArgExpr::Expr(expr),
                                ..
                            } => {
                                has_unsupported_modifiers = true;
                                Some(lower_expression(expr))
                            }
                            _ => {
                                has_unsupported_modifiers = true;
                                None
                            }
                        })
                        .collect()
                }
                FunctionArguments::None | FunctionArguments::Subquery(_) => Vec::new(),
            };
            ExpressionShape::Function {
                name: crate::object_name(&function.name),
                arguments,
                distinct,
                has_star_argument,
                has_unsupported_modifiers,
            }
        }
        Expr::UnaryOp { op, expr } => ExpressionShape::Unary {
            operator: op.to_string(),
            expression: Box::new(lower_expression(expr)),
        },
        Expr::BinaryOp { left, op, right } => ExpressionShape::Binary {
            operator: op.to_string(),
            left: Box::new(lower_expression(left)),
            right: Box::new(lower_expression(right)),
        },
        _ => ExpressionShape::Composite,
    };
    SqlExpression {
        shape,
        sql: expr.to_string(),
        debug: format!("{expr:?}"),
    }
}

fn lower_limit(query: &Query) -> Result<(Option<usize>, Option<usize>), SqlPlanError> {
    let mut limit = None;
    let mut offset = None;
    if let Some(clause) = &query.limit_clause {
        match clause {
            LimitClause::LimitOffset {
                limit: clause_limit,
                offset: clause_offset,
                limit_by,
            } => {
                if let Some(expr) = clause_limit {
                    limit = Some(nonnegative_integer(expr, "LIMIT")?);
                }
                if let Some(value) = clause_offset {
                    offset = Some(nonnegative_integer(&value.value, "OFFSET")?);
                }
                if !limit_by.is_empty() {
                    // Keep ClickHouse LIMIT BY outside the portable limit contract.
                    return Ok((limit, offset));
                }
            }
            LimitClause::OffsetCommaLimit {
                offset: clause_offset,
                limit: clause_limit,
            } => {
                offset = Some(nonnegative_integer(clause_offset, "OFFSET")?);
                limit = Some(nonnegative_integer(clause_limit, "LIMIT")?);
            }
        }
    }
    if limit.is_none()
        && let Some(fetch) = &query.fetch
        && let Some(quantity) = &fetch.quantity
    {
        limit = Some(nonnegative_integer(quantity, "LIMIT")?);
    }
    Ok((limit, offset))
}

fn nonnegative_integer(expr: &Expr, clause: &str) -> Result<usize, SqlPlanError> {
    let Expr::Value(ValueWithSpan {
        value: Value::Number(number, _),
        ..
    }) = expr
    else {
        return Err(SqlPlanError(format!(
            "SQL: {clause} must be a literal integer."
        )));
    };
    let value = number.parse::<i64>().map_err(|_| {
        SqlPlanError(format!(
            "SQL: {clause} must be a positive integer, got '{number}'"
        ))
    })?;
    if value < 0 {
        return Err(SqlPlanError(format!("SQL: {clause} must be non-negative.")));
    }
    usize::try_from(value)
        .map_err(|_| SqlPlanError(format!("SQL: {clause} value exceeds platform limit.")))
}
