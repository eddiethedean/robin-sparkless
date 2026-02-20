//! Implementations of robin-sparkless-core engine traits for the Polars backend.

use crate::dataframe::{DataFrame, GroupedData, JoinType as PlJoinType};
use crate::error::polars_to_core_error;
use crate::expr_ir::expr_ir_to_expr;
use crate::session::{DataFrameReader, SparkSession};
use polars::prelude::PolarsError;
use robin_sparkless_core::engine::{
    DataFrameBackend, DataFrameReaderBackend, GroupedDataBackend, SparkSessionBackend,
};
use robin_sparkless_core::error::EngineError as CoreEngineError;
use robin_sparkless_core::expr::ExprIr;
use robin_sparkless_core::schema::StructType;
use std::path::Path;

fn map_err(e: PolarsError) -> CoreEngineError {
    polars_to_core_error(e)
}

/// Core and polars EngineError are the same type; use for clarity in ? chains.
#[inline]
fn to_core(e: robin_sparkless_core::EngineError) -> CoreEngineError {
    e
}

impl DataFrameBackend for DataFrame {
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {
        self
    }

    fn filter(&self, condition: &ExprIr) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let expr = expr_ir_to_expr(condition).map_err(to_core)?;
        let df = self.filter(expr).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn select(&self, exprs: &[ExprIr]) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let exprs: Vec<_> = exprs
            .iter()
            .map(expr_ir_to_expr)
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_core)?;
        let df = self.select_exprs(exprs).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn select_columns(&self, columns: &[&str]) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let df = self.select(columns.to_vec()).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn with_column(
        &self,
        name: &str,
        expr: &ExprIr,
    ) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let e = expr_ir_to_expr(expr).map_err(to_core)?;
        let df = self.with_column_expr(name, e).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn join(
        &self,
        other: &dyn DataFrameBackend,
        on: &[&str],
        how: robin_sparkless_core::engine::JoinType,
    ) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let right = other
            .as_any()
            .downcast_ref::<DataFrame>()
            .ok_or_else(|| CoreEngineError::User("join only supported with same backend (Polars)".into()))?;
        let pl_how = match how {
            robin_sparkless_core::engine::JoinType::Inner => PlJoinType::Inner,
            robin_sparkless_core::engine::JoinType::Left => PlJoinType::Left,
            robin_sparkless_core::engine::JoinType::Right => PlJoinType::Right,
            robin_sparkless_core::engine::JoinType::Full => PlJoinType::Outer,
            robin_sparkless_core::engine::JoinType::LeftAnti => PlJoinType::LeftAnti,
            robin_sparkless_core::engine::JoinType::LeftSemi => PlJoinType::LeftSemi,
            robin_sparkless_core::engine::JoinType::Cross => {
                let df = self.cross_join(right).map_err(map_err)?;
                return Ok(Box::new(df));
            }
        };
        let df = self.join(right, on.to_vec(), pl_how).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn group_by(&self, column_names: &[&str]) -> Result<Box<dyn GroupedDataBackend>, CoreEngineError> {
        let g = self.group_by(column_names.to_vec()).map_err(map_err)?;
        Ok(Box::new(g))
    }

    fn order_by(
        &self,
        column_names: &[&str],
        ascending: &[bool],
    ) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let asc: Vec<bool> = ascending.to_vec();
        let df = self.order_by(column_names.to_vec(), asc).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn limit(&self, n: usize) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let df = self.limit(n).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn union(&self, other: &dyn DataFrameBackend) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let right = other
            .as_any()
            .downcast_ref::<DataFrame>()
            .ok_or_else(|| CoreEngineError::User("union only supported with same backend (Polars)".into()))?;
        let df = self.union(right).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn union_by_name(
        &self,
        other: &dyn DataFrameBackend,
        allow_missing_columns: bool,
    ) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let right = other
            .as_any()
            .downcast_ref::<DataFrame>()
            .ok_or_else(|| CoreEngineError::User("union_by_name only supported with same backend (Polars)".into()))?;
        let df = self.union_by_name(right, allow_missing_columns).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn distinct(
        &self,
        subset: Option<Vec<&str>>,
    ) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let df = self.distinct(subset).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn drop_columns(&self, columns: &[&str]) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let df = self.drop(columns.to_vec()).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn with_column_renamed(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let df = self.with_column_renamed(old_name, new_name).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn cross_join(&self, other: &dyn DataFrameBackend) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let right = other
            .as_any()
            .downcast_ref::<DataFrame>()
            .ok_or_else(|| CoreEngineError::User("cross_join only supported with same backend (Polars)".into()))?;
        let df = self.cross_join(right).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn collect(&self) -> Result<robin_sparkless_core::engine::CollectedRows, CoreEngineError> {
        self.collect_as_json_rows().map_err(map_err)
    }

    fn schema(&self) -> Result<StructType, CoreEngineError> {
        DataFrame::schema(self).map_err(map_err)
    }

    fn columns(&self) -> Result<Vec<String>, CoreEngineError> {
        DataFrame::columns(self).map_err(map_err)
    }

    fn count(&self) -> Result<u64, CoreEngineError> {
        let n = DataFrame::count(self).map_err(map_err)?;
        Ok(n as u64)
    }
}

impl GroupedDataBackend for GroupedData {
    fn agg(&self, exprs: &[ExprIr]) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let pl_exprs: Vec<_> = exprs
            .iter()
            .map(expr_ir_to_expr)
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_core)?;
        let df = self.agg(pl_exprs).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn count(&self) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let df = self.count().map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn sum(&self, column: &str) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let df = self.sum(column).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn min(&self, column: &str) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let df = self.min(column).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn max(&self, column: &str) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let df = self.max(column).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn mean(&self, column: &str) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let df = self.avg(&[column]).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn avg(&self, columns: &[&str]) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let df = self.avg(columns).map_err(map_err)?;
        Ok(Box::new(df))
    }
}

impl DataFrameReaderBackend for DataFrameReader {
    fn csv(&self, path: &Path) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let df = self.csv(path).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn parquet(&self, path: &Path) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let df = self.parquet(path).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn json(&self, path: &Path) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let df = self.json(path).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn table(&self, name: &str) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let df = self.table(name).map_err(map_err)?;
        Ok(Box::new(df))
    }
}

impl SparkSessionBackend for SparkSession {
    fn read(&self) -> Box<dyn DataFrameReaderBackend> {
        Box::new(DataFrameReader::new(self.clone()))
    }

    fn table(&self, name: &str) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let df = self.table(name).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn create_dataframe_from_rows(
        &self,
        rows: Vec<Vec<serde_json::Value>>,
        schema: Vec<(String, String)>,
    ) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let df = self
            .create_dataframe_from_rows(rows, schema)
            .map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn create_dataframe(
        &self,
        data: Vec<(i64, i64, String)>,
        column_names: Vec<&str>,
    ) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let df = self.create_dataframe(data, column_names).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn sql(&self, query: &str) -> Result<Box<dyn DataFrameBackend>, CoreEngineError> {
        let df = self.sql(query).map_err(map_err)?;
        Ok(Box::new(df))
    }

    fn register_table(&self, name: &str, df: &dyn DataFrameBackend) {
        let polars_df = df
            .as_any()
            .downcast_ref::<DataFrame>()
            .expect("register_table only supported with same backend (Polars)");
        SparkSession::register_table(self, name, polars_df.clone());
    }

    fn is_case_sensitive(&self) -> bool {
        self.is_case_sensitive()
    }

    fn get_config(&self) -> &std::collections::HashMap<String, String> {
        self.get_config()
    }
}
