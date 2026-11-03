//! Segregated, typed DataFrame capabilities.

use super::{CancellationToken, CollectedRows, ExecutionContext, JoinType};
use crate::error::EngineError;
use crate::expr::ExprIr;
use crate::schema::StructType;

/// Query construction operations. These methods preserve the concrete frame type and only
/// accept same-backend frames for binary operations.
pub trait FrameTransform: Send + Sync + Sized {
    type Grouped: GroupedData<Frame = Self>;

    fn filter(&self, condition: &ExprIr) -> Result<Self, EngineError>;
    fn select(&self, exprs: &[ExprIr]) -> Result<Self, EngineError>;
    fn select_columns(&self, columns: &[&str]) -> Result<Self, EngineError>;
    fn with_column(&self, name: &str, expr: &ExprIr) -> Result<Self, EngineError>;
    fn join(&self, other: &Self, on: &[&str], how: JoinType) -> Result<Self, EngineError>;
    fn group_by(&self, column_names: &[&str]) -> Result<Self::Grouped, EngineError>;
    fn order_by(&self, column_names: &[&str], ascending: &[bool]) -> Result<Self, EngineError>;
    fn limit(&self, n: usize) -> Result<Self, EngineError>;
    fn union(&self, other: &Self) -> Result<Self, EngineError>;
    fn union_by_name(&self, other: &Self, allow_missing_columns: bool)
    -> Result<Self, EngineError>;
    fn distinct(&self, subset: Option<Vec<&str>>) -> Result<Self, EngineError>;
    fn drop_columns(&self, columns: &[&str]) -> Result<Self, EngineError>;
    fn with_column_renamed(&self, old_name: &str, new_name: &str) -> Result<Self, EngineError>;
    fn cross_join(&self, other: &Self) -> Result<Self, EngineError>;
}

/// Materializing actions. Planning and transformation consumers do not need this capability.
pub trait FrameActions: Send + Sync {
    fn collect(&self) -> Result<CollectedRows, EngineError>;

    /// Cancellation is observed at documented backend safe points. Engines may not interrupt
    /// an in-progress non-interruptible call.
    fn collect_with_cancellation(
        &self,
        token: &CancellationToken,
    ) -> Result<CollectedRows, EngineError> {
        token.check_cancelled()?;
        let rows = self.collect()?;
        token.check_cancelled()?;
        Ok(rows)
    }

    fn collect_in_context(&self, context: &ExecutionContext) -> Result<CollectedRows, EngineError> {
        context.check_cancelled()?;
        self.collect_with_cancellation(context.cancellation())
    }

    fn count(&self) -> Result<u64, EngineError>;

    fn count_with_cancellation(&self, token: &CancellationToken) -> Result<u64, EngineError> {
        token.check_cancelled()?;
        let count = self.count()?;
        token.check_cancelled()?;
        Ok(count)
    }

    fn count_in_context(&self, context: &ExecutionContext) -> Result<u64, EngineError> {
        context.check_cancelled()?;
        self.count_with_cancellation(context.cancellation())
    }
}

/// Metadata-only frame operations. Analysis consumers do not need row materialization.
pub trait FrameSchema: Send + Sync {
    fn schema(&self) -> Result<StructType, EngineError>;
    fn columns(&self) -> Result<Vec<String>, EngineError>;
}

/// Aggregation capability returned by [`FrameTransform::group_by`].
pub trait GroupedData: Send + Sync {
    type Frame: FrameTransform + FrameActions + FrameSchema;

    fn agg(&self, exprs: &[ExprIr]) -> Result<Self::Frame, EngineError>;
    fn count(&self) -> Result<Self::Frame, EngineError>;
    fn sum(&self, column: &str) -> Result<Self::Frame, EngineError>;
    fn min(&self, column: &str) -> Result<Self::Frame, EngineError>;
    fn max(&self, column: &str) -> Result<Self::Frame, EngineError>;
    fn mean(&self, column: &str) -> Result<Self::Frame, EngineError>;
    fn avg(&self, columns: &[&str]) -> Result<Self::Frame, EngineError>;
}
