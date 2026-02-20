//! Root-owned DataFrame API; delegates to robin-sparkless-polars for execution.

use robin_sparkless_core::engine::{CollectedRows, DataFrameBackend, GroupedDataBackend};
use robin_sparkless_core::expr::ExprIr;
use robin_sparkless_core::EngineError;
use robin_sparkless_polars::dataframe::{
    DataFrameNa as PolarsDataFrameNa, DataFrameStat as PolarsDataFrameStat,
    DataFrameWriter as PolarsDataFrameWriter,
};
use robin_sparkless_polars::functions::SortOrder;
use robin_sparkless_polars::{
    Column, CubeRollupData as PolarsCubeRollupData, DataFrame as PolarsDataFrame, Expr,
    GroupedData as PolarsGroupedData, LazyFrame, PivotedGroupedData as PolarsPivotedGroupedData,
    PlDataFrame, PolarsError,
};
use robin_sparkless_core::{DataType, StructType};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use serde_json::Value as JsonValue;

/// Downcast trait result to root DataFrame (Polars backend).
fn downcast_df(box_df: Box<dyn DataFrameBackend>) -> Result<DataFrame, EngineError> {
    let concrete = box_df
        .as_any()
        .downcast_ref::<PolarsDataFrame>()
        .ok_or_else(|| EngineError::Internal("expected Polars backend".into()))?;
    Ok(DataFrame(concrete.clone()))
}

/// Root-owned DataFrame; delegates to the Polars backend.
#[derive(Clone)]
pub struct DataFrame(pub(crate) PolarsDataFrame);

/// Root-owned GroupedData; delegates to the Polars backend.
pub struct GroupedData(pub(crate) PolarsGroupedData);

/// Root-owned CubeRollupData; delegates to the Polars backend.
pub struct CubeRollupData(pub(crate) PolarsCubeRollupData);

/// Root-owned PivotedGroupedData; delegates to the Polars backend.
pub struct PivotedGroupedData(pub(crate) PolarsPivotedGroupedData);

/// Re-export for API compatibility.
pub use robin_sparkless_polars::dataframe::{
    JoinType, SaveMode, SelectItem, WriteFormat, WriteMode,
};

/// Root-owned DataFrameStat; delegates to the Polars backend.
pub struct DataFrameStat<'a>(PolarsDataFrameStat<'a>);

/// Root-owned DataFrameNa; delegates to the Polars backend.
pub struct DataFrameNa<'a>(PolarsDataFrameNa<'a>);

/// Root-owned DataFrameWriter; delegates to the Polars backend.
pub struct DataFrameWriter<'a> {
    inner: PolarsDataFrameWriter<'a>,
}

impl DataFrame {
    /// Create from a Polars DataFrame (via backend).
    pub fn from_polars(df: PlDataFrame) -> Self {
        DataFrame(PolarsDataFrame::from_polars(df))
    }

    /// Create from a Polars DataFrame with options.
    pub fn from_polars_with_options(df: PlDataFrame, case_sensitive: bool) -> Self {
        DataFrame(PolarsDataFrame::from_polars_with_options(
            df,
            case_sensitive,
        ))
    }

    /// Create from a LazyFrame (via backend).
    pub fn from_lazy(lf: LazyFrame) -> Self {
        DataFrame(PolarsDataFrame::from_lazy(lf))
    }

    /// Create from a LazyFrame with options.
    pub fn from_lazy_with_options(lf: LazyFrame, case_sensitive: bool) -> Self {
        DataFrame(PolarsDataFrame::from_lazy_with_options(lf, case_sensitive))
    }

    /// Create an empty DataFrame.
    pub fn empty() -> Self {
        DataFrame(PolarsDataFrame::empty())
    }

    /// Return a DataFrame with the given alias.
    pub fn alias(&self, name: &str) -> Self {
        DataFrame(self.0.alias(name))
    }

    /// Filter rows using an engine-agnostic expression (ExprIr).
    pub fn filter_expr_ir(&self, condition: &ExprIr) -> Result<DataFrame, EngineError> {
        downcast_df(DataFrameBackend::filter(&self.0, condition)?)
    }

    /// Select columns/expressions using ExprIr.
    pub fn select_expr_ir(&self, exprs: &[ExprIr]) -> Result<DataFrame, EngineError> {
        downcast_df(DataFrameBackend::select(&self.0, exprs)?)
    }

    /// Add or replace a column using ExprIr.
    pub fn with_column_expr_ir(&self, name: &str, expr: &ExprIr) -> Result<DataFrame, EngineError> {
        downcast_df(DataFrameBackend::with_column(&self.0, name, expr)?)
    }

    /// Collect as engine-agnostic rows (column name -> JSON value per row).
    pub fn collect_rows(&self) -> Result<CollectedRows, EngineError> {
        DataFrameBackend::collect(&self.0)
    }

    pub fn resolve_expr_column_names(&self, expr: Expr) -> Result<Expr, PolarsError> {
        self.0.resolve_expr_column_names(expr)
    }

    pub fn coerce_string_numeric_comparisons(&self, expr: Expr) -> Result<Expr, PolarsError> {
        self.0.coerce_string_numeric_comparisons(expr)
    }

    pub fn resolve_column_name(&self, name: &str) -> Result<String, PolarsError> {
        self.0.resolve_column_name(name)
    }

    pub fn schema(&self) -> Result<StructType, PolarsError> {
        self.0.schema()
    }

    pub fn schema_engine(&self) -> Result<StructType, EngineError> {
        self.0.schema_engine().map_err(Into::into)
    }

    pub fn get_column_dtype(&self, name: &str) -> Option<robin_sparkless_polars::PlDataType> {
        self.0.get_column_dtype(name)
    }

    pub fn get_column_data_type(&self, name: &str) -> Option<DataType> {
        self.0.get_column_data_type(name)
    }

    pub fn columns(&self) -> Result<Vec<String>, PolarsError> {
        self.0.columns()
    }

    pub fn columns_engine(&self) -> Result<Vec<String>, EngineError> {
        self.0.columns_engine().map_err(Into::into)
    }

    pub fn count(&self) -> Result<usize, PolarsError> {
        self.0.count()
    }

    pub fn count_engine(&self) -> Result<usize, EngineError> {
        self.0.count_engine().map_err(Into::into)
    }

    pub fn show(&self, n: Option<usize>) -> Result<(), PolarsError> {
        self.0.show(n)
    }

    pub fn collect(&self) -> Result<Arc<PlDataFrame>, PolarsError> {
        self.0.collect()
    }

    pub fn collect_as_json_rows_engine(
        &self,
    ) -> Result<Vec<HashMap<String, JsonValue>>, EngineError> {
        self.0.collect_as_json_rows_engine().map_err(Into::into)
    }

    pub fn collect_as_json_rows(&self) -> Result<Vec<HashMap<String, JsonValue>>, PolarsError> {
        self.0.collect_as_json_rows()
    }

    pub fn to_json_rows(&self) -> Result<String, EngineError> {
        self.0.to_json_rows().map_err(Into::into)
    }

    pub fn select_exprs(&self, exprs: Vec<Expr>) -> Result<DataFrame, PolarsError> {
        self.0.select_exprs(exprs).map(DataFrame)
    }

    pub fn select(&self, cols: Vec<&str>) -> Result<DataFrame, PolarsError> {
        self.0.select(cols).map(DataFrame)
    }

    pub fn select_engine(&self, cols: Vec<&str>) -> Result<DataFrame, EngineError> {
        self.0.select_engine(cols).map(DataFrame).map_err(Into::into)
    }

    pub fn select_items(&self, items: Vec<SelectItem<'_>>) -> Result<DataFrame, PolarsError> {
        self.0.select_items(items).map(DataFrame)
    }

    pub fn filter(&self, condition: Expr) -> Result<DataFrame, PolarsError> {
        self.0.filter(condition).map(DataFrame)
    }

    pub fn filter_engine(&self, condition: Expr) -> Result<DataFrame, EngineError> {
        self.0.filter_engine(condition).map(DataFrame).map_err(Into::into)
    }

    pub fn column(&self, name: &str) -> Result<Column, PolarsError> {
        self.0.column(name)
    }

    pub fn with_column(&self, column_name: &str, col: &Column) -> Result<DataFrame, PolarsError> {
        self.0.with_column(column_name, col).map(DataFrame)
    }

    pub fn with_column_engine(
        &self,
        column_name: &str,
        col: &Column,
    ) -> Result<DataFrame, EngineError> {
        self.0
            .with_column_engine(column_name, col)
            .map(DataFrame)
            .map_err(Into::into)
    }

    pub fn with_column_expr(
        &self,
        column_name: &str,
        expr: Expr,
    ) -> Result<DataFrame, PolarsError> {
        self.0.with_column_expr(column_name, expr).map(DataFrame)
    }

    pub fn group_by(&self, column_names: Vec<&str>) -> Result<GroupedData, PolarsError> {
        self.0.group_by(column_names).map(GroupedData)
    }

    pub fn group_by_engine(&self, column_names: Vec<&str>) -> Result<GroupedData, EngineError> {
        self.0
            .group_by_engine(column_names)
            .map(GroupedData)
            .map_err(Into::into)
    }

    pub fn group_by_exprs(
        &self,
        exprs: Vec<Expr>,
        grouping_col_names: Vec<String>,
    ) -> Result<GroupedData, PolarsError> {
        self.0
            .group_by_exprs(exprs, grouping_col_names)
            .map(GroupedData)
    }

    pub fn cube(&self, column_names: Vec<&str>) -> Result<CubeRollupData, PolarsError> {
        self.0.cube(column_names).map(CubeRollupData)
    }

    pub fn rollup(&self, column_names: Vec<&str>) -> Result<CubeRollupData, PolarsError> {
        self.0.rollup(column_names).map(CubeRollupData)
    }

    pub fn agg(&self, aggregations: Vec<Expr>) -> Result<DataFrame, PolarsError> {
        self.0.agg(aggregations).map(DataFrame)
    }

    pub fn join(
        &self,
        other: &DataFrame,
        on: Vec<&str>,
        how: JoinType,
    ) -> Result<DataFrame, PolarsError> {
        self.0.join(&other.0, on, how).map(DataFrame)
    }

    pub fn order_by(
        &self,
        column_names: Vec<&str>,
        ascending: Vec<bool>,
    ) -> Result<DataFrame, PolarsError> {
        self.0.order_by(column_names, ascending).map(DataFrame)
    }

    pub fn order_by_exprs(&self, sort_orders: Vec<SortOrder>) -> Result<DataFrame, PolarsError> {
        self.0.order_by_exprs(sort_orders).map(DataFrame)
    }

    pub fn union(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        self.0.union(&other.0).map(DataFrame)
    }

    pub fn union_all(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        self.0.union_all(&other.0).map(DataFrame)
    }

    pub fn union_by_name(
        &self,
        other: &DataFrame,
        allow_missing_columns: bool,
    ) -> Result<DataFrame, PolarsError> {
        self.0
            .union_by_name(&other.0, allow_missing_columns)
            .map(DataFrame)
    }

    pub fn distinct(&self, subset: Option<Vec<&str>>) -> Result<DataFrame, PolarsError> {
        self.0.distinct(subset).map(DataFrame)
    }

    pub fn drop(&self, columns: Vec<&str>) -> Result<DataFrame, PolarsError> {
        self.0.drop(columns).map(DataFrame)
    }

    pub fn dropna(
        &self,
        subset: Option<Vec<&str>>,
        how: &str,
        thresh: Option<usize>,
    ) -> Result<DataFrame, PolarsError> {
        self.0.dropna(subset, how, thresh).map(DataFrame)
    }

    pub fn fillna(&self, value: Expr, subset: Option<Vec<&str>>) -> Result<DataFrame, PolarsError> {
        self.0.fillna(value, subset).map(DataFrame)
    }

    pub fn limit(&self, n: usize) -> Result<DataFrame, PolarsError> {
        self.0.limit(n).map(DataFrame)
    }

    pub fn limit_engine(&self, n: usize) -> Result<DataFrame, EngineError> {
        self.0.limit_engine(n).map(DataFrame).map_err(Into::into)
    }

    pub fn with_column_renamed(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<DataFrame, PolarsError> {
        self.0
            .with_column_renamed(old_name, new_name)
            .map(DataFrame)
    }

    pub fn replace(
        &self,
        column_name: &str,
        old_value: Expr,
        new_value: Expr,
    ) -> Result<DataFrame, PolarsError> {
        self.0
            .replace(column_name, old_value, new_value)
            .map(DataFrame)
    }

    pub fn cross_join(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        self.0.cross_join(&other.0).map(DataFrame)
    }

    pub fn describe(&self) -> Result<DataFrame, PolarsError> {
        self.0.describe().map(DataFrame)
    }

    pub fn cache(&self) -> Result<DataFrame, PolarsError> {
        self.0.cache().map(DataFrame)
    }

    pub fn persist(&self) -> Result<DataFrame, PolarsError> {
        self.0.persist().map(DataFrame)
    }

    pub fn unpersist(&self) -> Result<DataFrame, PolarsError> {
        self.0.unpersist().map(DataFrame)
    }

    pub fn subtract(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        self.0.subtract(&other.0).map(DataFrame)
    }

    pub fn intersect(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        self.0.intersect(&other.0).map(DataFrame)
    }

    pub fn sample(
        &self,
        with_replacement: bool,
        fraction: f64,
        seed: Option<u64>,
    ) -> Result<DataFrame, PolarsError> {
        self.0
            .sample(with_replacement, fraction, seed)
            .map(DataFrame)
    }

    pub fn random_split(
        &self,
        weights: &[f64],
        seed: Option<u64>,
    ) -> Result<Vec<DataFrame>, PolarsError> {
        self.0
            .random_split(weights, seed)
            .map(|v| v.into_iter().map(DataFrame).collect())
    }

    pub fn sample_by(
        &self,
        col_name: &str,
        fractions: &[(Expr, f64)],
        seed: Option<u64>,
    ) -> Result<DataFrame, PolarsError> {
        self.0.sample_by(col_name, fractions, seed).map(DataFrame)
    }

    pub fn first(&self) -> Result<DataFrame, PolarsError> {
        self.0.first().map(DataFrame)
    }

    pub fn head(&self, n: usize) -> Result<DataFrame, PolarsError> {
        self.0.head(n).map(DataFrame)
    }

    pub fn take(&self, n: usize) -> Result<DataFrame, PolarsError> {
        self.0.take(n).map(DataFrame)
    }

    pub fn tail(&self, n: usize) -> Result<DataFrame, PolarsError> {
        self.0.tail(n).map(DataFrame)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn to_df(&self, names: Vec<&str>) -> Result<DataFrame, PolarsError> {
        self.0.to_df(names).map(DataFrame)
    }

    pub fn stat(&self) -> DataFrameStat<'_> {
        DataFrameStat(PolarsDataFrameStat::new(&self.0))
    }

    pub fn corr(&self) -> Result<DataFrame, PolarsError> {
        self.0.corr().map(DataFrame)
    }

    pub fn corr_cols(&self, col1: &str, col2: &str) -> Result<f64, PolarsError> {
        self.0.corr_cols(col1, col2)
    }

    pub fn cov_cols(&self, col1: &str, col2: &str) -> Result<f64, PolarsError> {
        self.0.cov_cols(col1, col2)
    }

    pub fn summary(&self) -> Result<DataFrame, PolarsError> {
        self.0.summary().map(DataFrame)
    }

    pub fn to_json(&self) -> Result<Vec<String>, PolarsError> {
        self.0.to_json()
    }

    pub fn explain(&self) -> String {
        self.0.explain()
    }

    pub fn print_schema(&self) -> Result<String, PolarsError> {
        self.0.print_schema()
    }

    pub fn checkpoint(&self) -> Result<DataFrame, PolarsError> {
        self.0.checkpoint().map(DataFrame)
    }

    pub fn local_checkpoint(&self) -> Result<DataFrame, PolarsError> {
        self.0.local_checkpoint().map(DataFrame)
    }

    pub fn repartition(&self, num_partitions: usize) -> Result<DataFrame, PolarsError> {
        self.0.repartition(num_partitions).map(DataFrame)
    }

    pub fn repartition_by_range(
        &self,
        num_partitions: usize,
        columns: Vec<&str>,
    ) -> Result<DataFrame, PolarsError> {
        self.0
            .repartition_by_range(num_partitions, columns)
            .map(DataFrame)
    }

    pub fn dtypes(&self) -> Result<Vec<(String, String)>, PolarsError> {
        self.0.dtypes()
    }

    pub fn sort_within_partitions(&self, cols: &[SortOrder]) -> Result<DataFrame, PolarsError> {
        self.0.sort_within_partitions(cols).map(DataFrame)
    }

    pub fn coalesce(&self, num_partitions: usize) -> Result<DataFrame, PolarsError> {
        self.0.coalesce(num_partitions).map(DataFrame)
    }

    pub fn hint(&self, name: &str, params: &[i32]) -> Result<DataFrame, PolarsError> {
        self.0.hint(name, params).map(DataFrame)
    }

    pub fn is_local(&self) -> bool {
        self.0.is_local()
    }

    pub fn input_files(&self) -> Vec<String> {
        self.0.input_files()
    }

    pub fn same_semantics(&self, other: &DataFrame) -> bool {
        self.0.same_semantics(&other.0)
    }

    pub fn semantic_hash(&self) -> u64 {
        self.0.semantic_hash()
    }

    pub fn observe(&self, name: &str, expr: Expr) -> Result<DataFrame, PolarsError> {
        self.0.observe(name, expr).map(DataFrame)
    }

    pub fn with_watermark(
        &self,
        event_time: &str,
        delay_threshold: &str,
    ) -> Result<DataFrame, PolarsError> {
        self.0
            .with_watermark(event_time, delay_threshold)
            .map(DataFrame)
    }

    pub fn select_expr(&self, exprs: &[String]) -> Result<DataFrame, PolarsError> {
        self.0.select_expr(exprs).map(DataFrame)
    }

    pub fn col_regex(&self, pattern: &str) -> Result<DataFrame, PolarsError> {
        self.0.col_regex(pattern).map(DataFrame)
    }

    pub fn with_columns(&self, exprs: &[(String, Column)]) -> Result<DataFrame, PolarsError> {
        self.0.with_columns(exprs).map(DataFrame)
    }

    pub fn with_columns_renamed(
        &self,
        renames: &[(String, String)],
    ) -> Result<DataFrame, PolarsError> {
        self.0.with_columns_renamed(renames).map(DataFrame)
    }

    pub fn na(&self) -> DataFrameNa<'_> {
        DataFrameNa(PolarsDataFrameNa::new(&self.0))
    }

    pub fn offset(&self, n: usize) -> Result<DataFrame, PolarsError> {
        self.0.offset(n).map(DataFrame)
    }

    pub fn transform<F>(&self, f: F) -> Result<DataFrame, PolarsError>
    where
        F: FnOnce(DataFrame) -> Result<DataFrame, PolarsError>,
    {
        self.0
            .transform(|polars_df| f(DataFrame(polars_df)).map(|r| r.0))
            .map(DataFrame)
    }

    pub fn freq_items(&self, columns: &[&str], support: f64) -> Result<DataFrame, PolarsError> {
        self.0.freq_items(columns, support).map(DataFrame)
    }

    pub fn approx_quantile(
        &self,
        column: &str,
        probabilities: &[f64],
    ) -> Result<DataFrame, PolarsError> {
        self.0.approx_quantile(column, probabilities).map(DataFrame)
    }

    pub fn crosstab(&self, col1: &str, col2: &str) -> Result<DataFrame, PolarsError> {
        self.0.crosstab(col1, col2).map(DataFrame)
    }

    /// Write this DataFrame to path (returns root-owned writer).
    pub fn write(&self) -> DataFrameWriter<'_> {
        DataFrameWriter {
            inner: self.0.write(),
        }
    }

    /// Write to Delta table (requires delta feature). Delegates to backend.
    #[cfg(feature = "delta")]
    pub fn write_delta(&self, path: impl AsRef<Path>, overwrite: bool) -> Result<(), PolarsError> {
        self.0.write_delta(path, overwrite)
    }

    #[cfg(not(feature = "delta"))]
    pub fn write_delta(
        &self,
        _path: impl AsRef<Path>,
        _overwrite: bool,
    ) -> Result<(), PolarsError> {
        Err(PolarsError::InvalidOperation(
            "Delta Lake requires the 'delta' feature. Build with --features delta.".into(),
        ))
    }

    /// Register as delta table by name.
    pub fn save_as_delta_table(&self, session: &crate::session::SparkSession, name: &str) {
        self.0.save_as_delta_table(&session.0, name)
    }
}

impl<'a> DataFrameStat<'a> {
    pub fn cov(&self, col1: &str, col2: &str) -> Result<f64, PolarsError> {
        self.0.cov(col1, col2)
    }

    pub fn corr(&self, col1: &str, col2: &str) -> Result<f64, PolarsError> {
        self.0.corr(col1, col2)
    }

    pub fn corr_matrix(&self) -> Result<DataFrame, PolarsError> {
        self.0.corr_matrix().map(DataFrame)
    }
}

// DataFrameNa - delegate to inner (methods return DataFrame, wrap)
impl<'a> DataFrameNa<'a> {
    pub fn drop(
        &self,
        subset: Option<Vec<&str>>,
        how: &str,
        thresh: Option<usize>,
    ) -> Result<DataFrame, PolarsError> {
        self.0.drop(subset, how, thresh).map(DataFrame)
    }

    pub fn fill(&self, value: Expr, subset: Option<Vec<&str>>) -> Result<DataFrame, PolarsError> {
        self.0.fill(value, subset).map(DataFrame)
    }

    pub fn replace(
        &self,
        old_value: Expr,
        new_value: Expr,
        subset: Option<Vec<&str>>,
    ) -> Result<DataFrame, PolarsError> {
        self.0.replace(old_value, new_value, subset).map(DataFrame)
    }
}

impl<'a> DataFrameWriter<'a> {
    pub fn mode(mut self, mode: WriteMode) -> Self {
        self.inner = self.inner.mode(mode);
        self
    }

    pub fn format(mut self, format: WriteFormat) -> Self {
        self.inner = self.inner.format(format);
        self
    }

    pub fn option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.inner = self.inner.option(key, value);
        self
    }

    pub fn options(mut self, opts: impl IntoIterator<Item = (String, String)>) -> Self {
        self.inner = self.inner.options(opts);
        self
    }

    pub fn partition_by(mut self, cols: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.inner = self.inner.partition_by(cols);
        self
    }

    pub fn save_as_table(
        &self,
        session: &crate::session::SparkSession,
        name: &str,
        mode: SaveMode,
    ) -> Result<(), PolarsError> {
        self.inner.save_as_table(&session.0, name, mode)
    }

    pub fn save(&self, path: impl AsRef<Path>) -> Result<(), PolarsError> {
        self.inner.save(path)
    }
}

impl GroupedData {
    pub fn count(&self) -> Result<DataFrame, PolarsError> {
        self.0.count().map(DataFrame)
    }

    pub fn sum(&self, column: &str) -> Result<DataFrame, PolarsError> {
        self.0.sum(column).map(DataFrame)
    }

    pub fn min(&self, column: &str) -> Result<DataFrame, PolarsError> {
        self.0.min(column).map(DataFrame)
    }

    pub fn max(&self, column: &str) -> Result<DataFrame, PolarsError> {
        self.0.max(column).map(DataFrame)
    }

    pub fn mean(&self, column: &str) -> Result<DataFrame, PolarsError> {
        self.0.avg(&[column]).map(DataFrame)
    }

    pub fn avg(&self, columns: &[&str]) -> Result<DataFrame, PolarsError> {
        self.0.avg(columns).map(DataFrame)
    }

    pub fn agg(&self, exprs: Vec<Expr>) -> Result<DataFrame, PolarsError> {
        self.0.agg(exprs).map(DataFrame)
    }

    pub fn agg_columns(&self, aggregations: Vec<Column>) -> Result<DataFrame, PolarsError> {
        self.0.agg_columns(aggregations).map(DataFrame)
    }

    /// Aggregate using ExprIr expressions (e.g. sum(col("x")), count(col("y")) from core).
    pub fn agg_expr_ir(&self, exprs: &[ExprIr]) -> Result<DataFrame, EngineError> {
        downcast_df(GroupedDataBackend::agg(&self.0, exprs)?)
    }

    pub fn pivot(&self, pivot_col: &str, values: Option<Vec<String>>) -> PivotedGroupedData {
        PivotedGroupedData(self.0.pivot(pivot_col, values))
    }
}

impl CubeRollupData {
    pub fn count(&self) -> Result<DataFrame, PolarsError> {
        self.0.count().map(DataFrame)
    }

    pub fn agg(&self, exprs: Vec<Expr>) -> Result<DataFrame, PolarsError> {
        self.0.agg(exprs).map(DataFrame)
    }
}

impl PivotedGroupedData {
    pub fn count(&self) -> Result<DataFrame, PolarsError> {
        self.0.count().map(DataFrame)
    }

    pub fn sum(&self, value_col: &str) -> Result<DataFrame, PolarsError> {
        self.0.sum(value_col).map(DataFrame)
    }

    pub fn avg(&self, value_col: &str) -> Result<DataFrame, PolarsError> {
        self.0.avg(value_col).map(DataFrame)
    }

    pub fn min(&self, value_col: &str) -> Result<DataFrame, PolarsError> {
        self.0.min(value_col).map(DataFrame)
    }

    pub fn max(&self, value_col: &str) -> Result<DataFrame, PolarsError> {
        self.0.max(value_col).map(DataFrame)
    }
}
