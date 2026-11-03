//! Typed implementations of the narrow core frame/session capabilities for Polars.

use crate::dataframe::{DataFrame, GroupedData, JoinType as PlJoinType};
use crate::error::polars_to_core_error;
use crate::expr_ir::expr_ir_to_expr;
use crate::session::{DataFrameReader, SparkSession};
use polars::prelude::PolarsError;
use robin_sparkless_core::engine::{
    CancellationToken, DataFrameReaderBackend, ExecutionContext, ExternalCatalog, FrameActions,
    FrameSchema, FrameTransform, GroupedData as CoreGroupedData, JoinType, SparkSessionBackend,
};
use robin_sparkless_core::error::EngineError;
use robin_sparkless_core::expr::ExprIr;
use std::path::Path;
use std::sync::Arc;

fn map_err(error: PolarsError) -> EngineError {
    polars_to_core_error(error)
}

impl FrameTransform for DataFrame {
    type Grouped = GroupedData;

    fn filter(&self, condition: &ExprIr) -> Result<Self, EngineError> {
        self.filter(expr_ir_to_expr(condition)?).map_err(map_err)
    }

    fn select(&self, exprs: &[ExprIr]) -> Result<Self, EngineError> {
        let exprs = exprs
            .iter()
            .map(expr_ir_to_expr)
            .collect::<Result<Vec<_>, _>>()?;
        self.select_exprs(exprs).map_err(map_err)
    }

    fn select_columns(&self, columns: &[&str]) -> Result<Self, EngineError> {
        self.select(columns.to_vec()).map_err(map_err)
    }

    fn with_column(&self, name: &str, expr: &ExprIr) -> Result<Self, EngineError> {
        self.with_column_expr(name, expr_ir_to_expr(expr)?)
            .map_err(map_err)
    }

    fn join(&self, other: &Self, on: &[&str], how: JoinType) -> Result<Self, EngineError> {
        let how = match how {
            JoinType::Inner => PlJoinType::Inner,
            JoinType::Left => PlJoinType::Left,
            JoinType::Right => PlJoinType::Right,
            JoinType::Full => PlJoinType::Outer,
            JoinType::LeftAnti => PlJoinType::LeftAnti,
            JoinType::LeftSemi => PlJoinType::LeftSemi,
            JoinType::Cross => return self.cross_join(other).map_err(map_err),
        };
        self.join(other, on.to_vec(), how).map_err(map_err)
    }

    fn group_by(&self, column_names: &[&str]) -> Result<Self::Grouped, EngineError> {
        self.group_by(column_names.to_vec()).map_err(map_err)
    }

    fn order_by(&self, column_names: &[&str], ascending: &[bool]) -> Result<Self, EngineError> {
        self.order_by(column_names.to_vec(), ascending.to_vec())
            .map_err(map_err)
    }

    fn limit(&self, n: usize) -> Result<Self, EngineError> {
        self.limit(n).map_err(map_err)
    }

    fn union(&self, other: &Self) -> Result<Self, EngineError> {
        self.union(other).map_err(map_err)
    }

    fn union_by_name(
        &self,
        other: &Self,
        allow_missing_columns: bool,
    ) -> Result<Self, EngineError> {
        self.union_by_name(other, allow_missing_columns)
            .map_err(map_err)
    }

    fn distinct(&self, subset: Option<Vec<&str>>) -> Result<Self, EngineError> {
        self.distinct(subset).map_err(map_err)
    }

    fn drop_columns(&self, columns: &[&str]) -> Result<Self, EngineError> {
        self.drop(columns.to_vec()).map_err(map_err)
    }

    fn with_column_renamed(&self, old_name: &str, new_name: &str) -> Result<Self, EngineError> {
        self.with_column_renamed(old_name, new_name)
            .map_err(map_err)
    }

    fn cross_join(&self, other: &Self) -> Result<Self, EngineError> {
        self.cross_join(other).map_err(map_err)
    }
}

impl FrameActions for DataFrame {
    fn collect(&self) -> Result<robin_sparkless_core::engine::CollectedRows, EngineError> {
        self.collect_as_json_rows().map_err(map_err)
    }

    fn collect_with_cancellation(
        &self,
        token: &CancellationToken,
    ) -> Result<robin_sparkless_core::engine::CollectedRows, EngineError> {
        self.collect_as_json_rows_with_cancellation(token)
    }

    fn count(&self) -> Result<u64, EngineError> {
        DataFrame::count(self)
            .map(|count| count as u64)
            .map_err(map_err)
    }

    fn count_with_cancellation(&self, token: &CancellationToken) -> Result<u64, EngineError> {
        DataFrame::count_with_cancellation(self, token).map(|count| count as u64)
    }
}

impl FrameSchema for DataFrame {
    fn schema(&self) -> Result<robin_sparkless_core::schema::StructType, EngineError> {
        DataFrame::schema(self).map_err(map_err)
    }

    fn columns(&self) -> Result<Vec<String>, EngineError> {
        DataFrame::columns(self).map_err(map_err)
    }
}

impl CoreGroupedData for GroupedData {
    type Frame = DataFrame;

    fn agg(&self, exprs: &[ExprIr]) -> Result<Self::Frame, EngineError> {
        let exprs = exprs
            .iter()
            .map(expr_ir_to_expr)
            .collect::<Result<Vec<_>, _>>()?;
        self.agg(exprs).map_err(map_err)
    }

    fn count(&self) -> Result<Self::Frame, EngineError> {
        self.count().map_err(map_err)
    }

    fn sum(&self, column: &str) -> Result<Self::Frame, EngineError> {
        self.sum(column).map_err(map_err)
    }

    fn min(&self, column: &str) -> Result<Self::Frame, EngineError> {
        self.min(column).map_err(map_err)
    }

    fn max(&self, column: &str) -> Result<Self::Frame, EngineError> {
        self.max(column).map_err(map_err)
    }

    fn mean(&self, column: &str) -> Result<Self::Frame, EngineError> {
        self.avg(&[column]).map_err(map_err)
    }

    fn avg(&self, columns: &[&str]) -> Result<Self::Frame, EngineError> {
        self.avg(columns).map_err(map_err)
    }
}

impl DataFrameReaderBackend for DataFrameReader {
    type Frame = DataFrame;

    fn csv(&self, path: &Path) -> Result<Self::Frame, EngineError> {
        self.csv(path).map_err(map_err)
    }

    fn parquet(&self, path: &Path) -> Result<Self::Frame, EngineError> {
        self.parquet(path).map_err(map_err)
    }

    fn json(&self, path: &Path) -> Result<Self::Frame, EngineError> {
        self.json(path).map_err(map_err)
    }

    fn table(&self, name: &str) -> Result<Self::Frame, EngineError> {
        self.table(name).map_err(map_err)
    }
}

impl SparkSessionBackend for SparkSession {
    type Frame = DataFrame;
    type Reader = DataFrameReader;

    fn read(&self) -> Self::Reader {
        DataFrameReader::new(self.clone())
    }

    fn table(&self, name: &str) -> Result<Self::Frame, EngineError> {
        SparkSession::table(self, name).map_err(map_err)
    }

    fn create_dataframe_from_rows(
        &self,
        rows: Vec<Vec<serde_json::Value>>,
        schema: Vec<(String, String)>,
        verify_schema: bool,
        schema_was_inferred: bool,
    ) -> Result<Self::Frame, EngineError> {
        SparkSession::create_dataframe_from_rows(
            self,
            rows,
            schema,
            verify_schema,
            schema_was_inferred,
        )
        .map_err(map_err)
    }

    fn sql(&self, query: &str) -> Result<Self::Frame, EngineError> {
        SparkSession::sql(self, query).map_err(map_err)
    }

    fn sql_with_cancellation(
        &self,
        query: &str,
        token: &CancellationToken,
    ) -> Result<Self::Frame, EngineError> {
        SparkSession::sql_with_cancellation(self, query, token)
    }

    fn sql_with_context(
        &self,
        query: &str,
        context: &ExecutionContext,
    ) -> Result<Self::Frame, EngineError> {
        SparkSession::sql_with_context(self, query, context)
    }

    fn register_table(&self, name: &str, df: &Self::Frame) -> Result<(), EngineError> {
        SparkSession::register_table(self, name, df.clone());
        Ok(())
    }

    fn set_external_catalog(
        &self,
        catalog: Option<Arc<ExternalCatalog>>,
    ) -> Result<(), EngineError> {
        SparkSession::set_external_catalog(self, catalog)
    }

    fn is_case_sensitive(&self) -> bool {
        SparkSession::is_case_sensitive(self)
    }

    fn get_config(&self) -> &std::collections::HashMap<String, String> {
        SparkSession::get_config(self)
    }
}
