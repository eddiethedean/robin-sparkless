use serde_json::{Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationKind {
    Stop,
    Filter,
    Select,
    Limit,
    Offset,
    OrderBy,
    Distinct,
    Drop,
    WithColumnRenamed,
    WithColumn,
    GroupBy,
    Join,
    Union,
    UnionByName,
    CrossJoin,
    Rollup,
    Cube,
}

impl OperationKind {
    pub(crate) fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "stop" => Self::Stop,
            "filter" => Self::Filter,
            "select" => Self::Select,
            "limit" => Self::Limit,
            "offset" => Self::Offset,
            "orderBy" => Self::OrderBy,
            "distinct" => Self::Distinct,
            "drop" => Self::Drop,
            "withColumnRenamed" => Self::WithColumnRenamed,
            "withColumn" => Self::WithColumn,
            "groupBy" => Self::GroupBy,
            "join" => Self::Join,
            "union" => Self::Union,
            "unionByName" => Self::UnionByName,
            "crossJoin" | "cross_join" => Self::CrossJoin,
            "rollup" => Self::Rollup,
            "cube" => Self::Cube,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Filter => "filter",
            Self::Select => "select",
            Self::Limit => "limit",
            Self::Offset => "offset",
            Self::OrderBy => "orderBy",
            Self::Distinct => "distinct",
            Self::Drop => "drop",
            Self::WithColumnRenamed => "withColumnRenamed",
            Self::WithColumn => "withColumn",
            Self::GroupBy => "groupBy",
            Self::Join => "join",
            Self::Union => "union",
            Self::UnionByName => "unionByName",
            Self::CrossJoin => "crossJoin",
            Self::Rollup => "rollup",
            Self::Cube => "cube",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PlanOperation {
    pub(crate) kind: OperationKind,
    pub(crate) payload: PlanPayload,
}

/// Operation-specific payloads after the decoder has checked every required field.
#[derive(Debug, Clone)]
pub enum PlanPayload {
    Empty,
    Expression(PlanExpression),
    Select(Vec<Value>),
    Limit {
        n: u64,
    },
    Offset {
        n: u64,
    },
    OrderBy {
        columns: Vec<Value>,
        nulls_last: Option<Vec<Value>>,
    },
    Drop {
        columns: Vec<Value>,
    },
    Rename {
        old: String,
        new: String,
    },
    WithColumn {
        name: String,
        expression: PlanExpression,
    },
    GroupBy {
        keys: Vec<Value>,
        aggregations: Vec<Value>,
    },
    Join(BinaryInput),
    Union(BinaryInput),
    UnionByName(BinaryInput),
    CrossJoin(BinaryInput),
    Rollup(GroupInput),
    Cube(GroupInput),
}

#[derive(Debug, Clone)]
pub struct BinaryInput {
    pub data: Vec<Value>,
    pub schema: Vec<Value>,
    pub on: Option<Value>,
    pub how: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GroupInput {
    pub keys: Vec<Value>,
    pub aggregations: Vec<Value>,
}

/// A validated, engine-neutral expression tree. `source` preserves wire details that remain
/// frontend-specific (for example a function alias spelling) for backend adapters.
#[derive(Debug, Clone)]
pub struct PlanExpression {
    pub kind: ExpressionKind,
    pub(crate) source: Value,
}

#[derive(Debug, Clone)]
pub enum ExpressionKind {
    Column(String),
    Literal,
    Function {
        name: String,
        arguments: Vec<PlanExpression>,
    },
    Operator {
        name: String,
        arguments: Vec<PlanExpression>,
    },
    Sequence(Vec<PlanExpression>),
}

impl PlanExpression {
    pub fn to_json_value(&self) -> Value {
        self.source.clone()
    }
}

impl PlanOperation {
    pub fn kind(&self) -> OperationKind {
        self.kind
    }

    pub fn payload(&self) -> &PlanPayload {
        &self.payload
    }

    /// Restore the normalized wire payload consumed by execution adapters.
    pub fn payload_json(&self) -> Value {
        match &self.payload {
            PlanPayload::Empty => Value::Null,
            PlanPayload::Expression(expression) => expression.to_json_value(),
            PlanPayload::Select(items) => Value::Array(items.clone()),
            PlanPayload::Limit { n } | PlanPayload::Offset { n } => {
                serde_json::json!({"n": n})
            }
            PlanPayload::OrderBy {
                columns,
                nulls_last,
            } => {
                let mut payload = Map::new();
                payload.insert("columns".into(), Value::Array(columns.clone()));
                if let Some(nulls_last) = nulls_last {
                    payload.insert("nulls_last".into(), Value::Array(nulls_last.clone()));
                }
                Value::Object(payload)
            }
            PlanPayload::Drop { columns } => serde_json::json!({"columns": columns}),
            PlanPayload::Rename { old, new } => serde_json::json!({"old": old, "new": new}),
            PlanPayload::WithColumn { name, expression } => {
                serde_json::json!({"name": name, "expr": expression.to_json_value()})
            }
            PlanPayload::GroupBy { keys, aggregations } => {
                serde_json::json!({"group_by": keys, "aggs": aggregations})
            }
            PlanPayload::Join(input) => binary_payload(input, true),
            PlanPayload::Union(input)
            | PlanPayload::UnionByName(input)
            | PlanPayload::CrossJoin(input) => binary_payload(input, false),
            PlanPayload::Rollup(input) | PlanPayload::Cube(input) => {
                serde_json::json!({"group_by": input.keys, "aggs": input.aggregations})
            }
        }
    }
}

fn binary_payload(input: &BinaryInput, include_join: bool) -> Value {
    let mut payload = Map::new();
    payload.insert("other_data".into(), Value::Array(input.data.clone()));
    payload.insert("other_schema".into(), Value::Array(input.schema.clone()));
    if include_join {
        if let Some(on) = &input.on {
            payload.insert("on".into(), on.clone());
        }
        if let Some(how) = &input.how {
            payload.insert("how".into(), Value::String(how.clone()));
        }
    }
    Value::Object(payload)
}

#[derive(Debug, Clone, Default)]
pub struct LogicalPlan {
    pub operations: Vec<PlanOperation>,
}
