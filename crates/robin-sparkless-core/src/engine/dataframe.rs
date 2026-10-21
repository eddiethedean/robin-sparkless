//! Engine-agnostic DataFrame backend trait.

use super::{CancellationToken, CollectedRows, JoinType};
use crate::error::EngineError;
use crate::expr::ExprIr;
use crate::schema::StructType;

/// Backend for GroupedData (result of group_by).
pub trait GroupedDataBackend: Send + Sync {
    fn agg(&self, exprs: &[ExprIr]) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn count(&self) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn sum(&self, column: &str) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn min(&self, column: &str) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn max(&self, column: &str) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn mean(&self, column: &str) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn avg(&self, columns: &[&str]) -> Result<Box<dyn DataFrameBackend>, EngineError>;
}

/// Backend for DataFrame operations. All expression arguments use ExprIr.
/// Implementors can be downcast via `as_any()` for backend-specific operations (e.g. join).
pub trait DataFrameBackend: Send + Sync {
    /// For downcasting to concrete backend type (e.g. for join with same backend).
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync);

    fn filter(&self, condition: &ExprIr) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn select(&self, exprs: &[ExprIr]) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn select_columns(&self, columns: &[&str]) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn with_column(
        &self,
        name: &str,
        expr: &ExprIr,
    ) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn join(
        &self,
        other: &dyn DataFrameBackend,
        on: &[&str],
        how: JoinType,
    ) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn group_by(&self, column_names: &[&str]) -> Result<Box<dyn GroupedDataBackend>, EngineError>;
    fn order_by(
        &self,
        column_names: &[&str],
        ascending: &[bool],
    ) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn limit(&self, n: usize) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn union(&self, other: &dyn DataFrameBackend)
    -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn union_by_name(
        &self,
        other: &dyn DataFrameBackend,
        allow_missing_columns: bool,
    ) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn distinct(&self, subset: Option<Vec<&str>>)
    -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn drop_columns(&self, columns: &[&str]) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn with_column_renamed(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn cross_join(
        &self,
        other: &dyn DataFrameBackend,
    ) -> Result<Box<dyn DataFrameBackend>, EngineError>;

    fn collect(&self) -> Result<CollectedRows, EngineError>;
    /// Collect while checking a caller-owned cancellation token at backend safe points.
    ///
    /// The default checks before and after collection. Engines with finer-grained safe
    /// points may override this method. Cancellation requested inside a non-interruptible
    /// engine call is reported only after that call returns.
    fn collect_with_cancellation(
        &self,
        token: &CancellationToken,
    ) -> Result<CollectedRows, EngineError> {
        token.check_cancelled()?;
        let rows = self.collect()?;
        token.check_cancelled()?;
        Ok(rows)
    }
    fn schema(&self) -> Result<StructType, EngineError>;
    fn columns(&self) -> Result<Vec<String>, EngineError>;
    fn count(&self) -> Result<u64, EngineError>;
    /// Count rows while checking a caller-owned cancellation token at safe points.
    fn count_with_cancellation(&self, token: &CancellationToken) -> Result<u64, EngineError> {
        token.check_cancelled()?;
        let count = self.count()?;
        token.check_cancelled()?;
        Ok(count)
    }
}
