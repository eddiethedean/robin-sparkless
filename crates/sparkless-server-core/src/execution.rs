//! Engine-neutral execution boundary shared by server runtime and adapters.

use robin_sparkless_core::engine::{ArrowIpcBatchStream, CancellationToken};
use robin_sparkless_core::schema::StructType;
use serde_json::Value;
use std::any::Any;
use std::sync::Arc;

/// Opaque engine-owned session state. The adapter validates the owner before downcasting.
#[derive(Clone)]
pub struct EngineSessionHandle {
    owner: &'static str,
    state: Arc<dyn Any + Send + Sync>,
}

impl EngineSessionHandle {
    pub fn new<T: Any + Send + Sync>(owner: &'static str, state: T) -> Self {
        Self {
            owner,
            state: Arc::new(state),
        }
    }

    pub fn owner(&self) -> &'static str {
        self.owner
    }

    pub fn downcast_ref<T: Any>(&self, expected_owner: &'static str) -> Option<&T> {
        (self.owner == expected_owner)
            .then(|| self.state.downcast_ref::<T>())
            .flatten()
    }
}

/// Request data needed by the supported SQL execution surface.
#[derive(Debug, Clone)]
pub struct SqlExecutionRequest {
    pub sql: String,
    /// The single parsed statement accepted by the request validator. Backends may execute this
    /// AST directly so validation, semantics, and output metadata share one parse.
    pub statement: Option<robin_sparkless_sql::SparkStatement>,
    pub action: String,
    pub target: Option<String>,
    /// Projection names and nullability derived while validating a parsed SQL statement.
    /// Adapters can preserve the declared SQL output schema without reparsing the statement.
    pub projection: Option<SqlProjectionMetadata>,
    pub max_rows: usize,
    pub max_result_bytes: usize,
    pub max_cell_bytes: usize,
    pub cancellation: CancellationToken,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlProjectionMetadata {
    pub names: Option<Vec<Option<String>>>,
    pub nullable: Option<Vec<bool>>,
}

/// Stable server-side failure returned by an execution adapter.
#[derive(Debug, Clone)]
pub struct ExecutionError {
    pub code: &'static str,
    pub message: String,
    pub details: Option<Value>,
}

/// A SQL result is transport-neutral structured data. HTTP and Connect own their wire encoding.
#[derive(Debug, Clone)]
pub struct SqlExecutionResult {
    pub payload: Value,
    /// Optional serialized JSON retained by the blocking execution worker after applying the
    /// same response byte limit as `payload`. HTTP transports can send it without re-encoding on
    /// the async connection task.
    pub encoded_json: Option<Vec<u8>>,
}

/// The only engine-specific call exposed to server request orchestration.
pub trait ExecutionPort: Send + Sync {
    fn execute_sql(
        &self,
        session: &EngineSessionHandle,
        request: SqlExecutionRequest,
    ) -> Result<SqlExecutionResult, ExecutionError>;
}

#[derive(Debug, Clone)]
pub struct ConnectExecutionRequest {
    pub plan: ConnectExecutionPlan,
    /// Stable owner-level identity for mutations. The operation key is scoped to the client
    /// session and Connect operation id; the request fingerprint prevents conflicting reuse.
    pub operation_identity: ConnectOperationIdentity,
    pub max_batch_rows: usize,
    pub max_response_bytes: usize,
    pub cancellation: CancellationToken,
}

#[derive(Debug, Clone)]
pub struct ConnectOperationIdentity {
    pub key: String,
    pub request_fingerprint: [u8; 32],
}

pub struct ConnectExecutionResult {
    /// Logical output schema. Effect-only commands have no Arrow result.
    pub schema: Option<StructType>,
    pub batches: ArrowIpcBatchStream,
}

/// Transport-neutral plan received by the Connect execution adapter.
#[derive(Debug, Clone)]
pub enum ConnectExecutionPlan {
    Root(ConnectRelation),
    Command(ConnectCommand),
    Missing,
}

#[derive(Debug, Clone)]
pub enum ConnectCommand {
    Sql { sql: String },
    Unsupported(String),
}

#[derive(Debug, Clone)]
pub struct ConnectRelation {
    pub plan_id: Option<i64>,
    pub kind: ConnectRelationKind,
}

#[derive(Debug, Clone)]
pub enum ConnectRelationKind {
    Range {
        start: Option<i64>,
        end: i64,
        step: i64,
        partitions: Option<i32>,
    },
    Local {
        data: Option<Vec<u8>>,
        schema: Option<String>,
    },
    CachedLocal,
    Read {
        is_streaming: bool,
        named_table: Option<ConnectNamedTable>,
    },
    Alias {
        input: Option<Box<ConnectRelation>>,
        alias: String,
        qualifier: Vec<String>,
    },
    Filter {
        input: Option<Box<ConnectRelation>>,
        condition: Option<ConnectExpression>,
    },
    Project {
        input: Option<Box<ConnectRelation>>,
        expressions: Vec<ConnectExpression>,
    },
    Limit {
        input: Option<Box<ConnectRelation>>,
        limit: i64,
    },
    Join {
        left: Option<Box<ConnectRelation>>,
        right: Option<Box<ConnectRelation>>,
        condition: Option<ConnectExpression>,
        join_type: ConnectJoinType,
        using_columns: Vec<String>,
        has_join_data_type: bool,
    },
    Aggregate {
        input: Option<Box<ConnectRelation>>,
        group_type: ConnectGroupType,
        grouping_expressions: Vec<ConnectExpression>,
        aggregate_expressions: Vec<ConnectExpression>,
        has_pivot: bool,
        has_grouping_sets: bool,
    },
    Sql {
        query: String,
        has_arguments: bool,
    },
    Unsupported(String),
    Missing,
}

#[derive(Debug, Clone)]
pub struct ConnectNamedTable {
    pub identifier: String,
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectJoinType {
    Unspecified,
    Inner,
    FullOuter,
    LeftOuter,
    RightOuter,
    LeftAnti,
    LeftSemi,
    Cross,
    Unknown(i32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectGroupType {
    GroupBy,
    Other(i32),
}

#[derive(Debug, Clone)]
pub enum ConnectExpression {
    Literal(ConnectLiteral),
    Attribute {
        identifier: String,
        plan_id: Option<i64>,
        is_metadata_column: bool,
    },
    Function {
        name: String,
        arguments: Vec<ConnectExpression>,
        is_distinct: bool,
        is_user_defined: bool,
        is_internal: bool,
    },
    ExpressionString(String),
    Alias {
        expression: Option<Box<ConnectExpression>>,
        names: Vec<String>,
        metadata: Option<String>,
    },
    Star,
    Unsupported(String),
    Missing,
}

#[derive(Debug, Clone)]
pub enum ConnectLiteral {
    Integer(i32),
    Long(i64),
    Boolean(bool),
    Double(f64),
    String(String),
    Null,
    Unsupported(String),
    Missing,
}

pub trait ConnectExecutionPort: Send + Sync {
    fn execute_connect_plan(
        &self,
        session: &EngineSessionHandle,
        request: ConnectExecutionRequest,
    ) -> Result<ConnectExecutionResult, ExecutionError>;
}
