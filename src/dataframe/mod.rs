//! DataFrame module: main tabular type and submodules for transformations, aggregations, joins, stats.

mod aggregations;
mod joins;
mod stats;
mod transformations;

pub use aggregations::{CubeRollupData, GroupedData};
pub use joins::{join, JoinType};
pub use stats::DataFrameStat;
pub use transformations::{
    filter, order_by, order_by_exprs, select, select_with_exprs, with_column, DataFrameNa,
};

use crate::column::Column;
use crate::functions::SortOrder;
use crate::schema::StructType;
use crate::session::SparkSession;
use polars::prelude::{
    col, lit, AnyValue, DataFrame as PlDataFrame, DataType, Expr, PlSmallStr, PolarsError,
    SchemaNamesAndDtypes,
};
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

/// Default for `spark.sql.caseSensitive` (PySpark default is false = case-insensitive).
const DEFAULT_CASE_SENSITIVE: bool = false;

/// DataFrame - main tabular data structure.
/// Thin wrapper around an eager Polars `DataFrame`.
pub struct DataFrame {
    pub(crate) df: Arc<PlDataFrame>,
    /// When false (default), column names are matched case-insensitively (PySpark behavior).
    pub(crate) case_sensitive: bool,
}

impl DataFrame {
    /// Create a new DataFrame from a Polars DataFrame (case-insensitive column matching by default).
    pub fn from_polars(df: PlDataFrame) -> Self {
        DataFrame {
            df: Arc::new(df),
            case_sensitive: DEFAULT_CASE_SENSITIVE,
        }
    }

    /// Create a new DataFrame from a Polars DataFrame with explicit case sensitivity.
    /// When `case_sensitive` is false, column resolution is case-insensitive (PySpark default).
    pub fn from_polars_with_options(df: PlDataFrame, case_sensitive: bool) -> Self {
        DataFrame {
            df: Arc::new(df),
            case_sensitive,
        }
    }

    /// Create an empty DataFrame
    pub fn empty() -> Self {
        DataFrame {
            df: Arc::new(PlDataFrame::empty()),
            case_sensitive: DEFAULT_CASE_SENSITIVE,
        }
    }

    /// Resolve column names in a Polars expression against this DataFrame's schema.
    /// When case_sensitive is false, column references (e.g. col("name")) are resolved
    /// case-insensitively (PySpark default). Use before filter/select_with_exprs/order_by_exprs.
    /// Names that appear as alias outputs (e.g. in expr.alias("partial")) are not resolved
    /// as input columns, so select(col("x").substr(1, 3).alias("partial")) works (issue #200).
    pub fn resolve_expr_column_names(&self, expr: Expr) -> Result<Expr, PolarsError> {
        let df = self;
        let mut alias_output_names: HashSet<String> = HashSet::new();
        let _ = expr.clone().try_map_expr(|e| {
            if let Expr::Alias(_, name) = &e {
                alias_output_names.insert(name.as_str().to_string());
            }
            Ok(e)
        })?;
        expr.try_map_expr(move |e| {
            if let Expr::Column(name) = &e {
                let name_str = name.as_str();
                if alias_output_names.contains(name_str) {
                    return Ok(e);
                }
                let resolved = df.resolve_column_name(name_str)?;
                return Ok(Expr::Column(PlSmallStr::from(resolved.as_str())));
            }
            Ok(e)
        })
    }

    /// Resolve a logical column name to the actual column name in the schema.
    /// When case_sensitive is false, matches case-insensitively.
    pub fn resolve_column_name(&self, name: &str) -> Result<String, PolarsError> {
        let names = self.df.get_column_names();
        if self.case_sensitive {
            if names.iter().any(|n| *n == name) {
                return Ok(name.to_string());
            }
        } else {
            let name_lower = name.to_lowercase();
            for n in names {
                if n.to_lowercase() == name_lower {
                    return Ok(n.to_string());
                }
            }
        }
        let available: Vec<String> = self
            .df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        Err(PolarsError::ColumnNotFound(
            format!(
                "Column '{}' not found. Available columns: [{}]. Check spelling and case sensitivity (spark.sql.caseSensitive).",
                name,
                available.join(", ")
            )
            .into(),
        ))
    }

    /// Get the schema of the DataFrame
    pub fn schema(&self) -> Result<StructType, PolarsError> {
        Ok(StructType::from_polars_schema(&self.df.schema()))
    }

    /// Get column names
    pub fn columns(&self) -> Result<Vec<String>, PolarsError> {
        Ok(self
            .df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect())
    }

    /// Count the number of rows (action - triggers execution)
    pub fn count(&self) -> Result<usize, PolarsError> {
        Ok(self.df.height())
    }

    /// Show the first n rows
    pub fn show(&self, n: Option<usize>) -> Result<(), PolarsError> {
        let n = n.unwrap_or(20);
        println!("{}", self.df.head(Some(n)));
        Ok(())
    }

    /// Collect the DataFrame (action - triggers execution)
    pub fn collect(&self) -> Result<Arc<PlDataFrame>, PolarsError> {
        Ok(self.df.clone())
    }

    /// Collect as rows of column-name -> JSON value. For use by language bindings (Node, etc.).
    pub fn collect_as_json_rows(&self) -> Result<Vec<HashMap<String, JsonValue>>, PolarsError> {
        let df = self.df.as_ref();
        let names = df.get_column_names();
        let nrows = df.height();
        let mut rows = Vec::with_capacity(nrows);
        for i in 0..nrows {
            let mut row = HashMap::with_capacity(names.len());
            for (col_idx, name) in names.iter().enumerate() {
                let s = df
                    .get_columns()
                    .get(col_idx)
                    .ok_or_else(|| PolarsError::ComputeError("column index out of range".into()))?;
                let av = s.get(i)?;
                let jv = any_value_to_json(av);
                row.insert(name.to_string(), jv);
            }
            rows.push(row);
        }
        Ok(rows)
    }

    /// Select columns (returns a new DataFrame).
    /// Accepts either column names (strings) or Column expressions (e.g. from regexp_extract_all(...).alias("m")).
    /// Column names are resolved according to case sensitivity.
    pub fn select_exprs(&self, exprs: Vec<Expr>) -> Result<DataFrame, PolarsError> {
        transformations::select_with_exprs(self, exprs, self.case_sensitive)
    }

    /// Select columns by name (returns a new DataFrame).
    /// Column names are resolved according to case sensitivity.
    pub fn select(&self, cols: Vec<&str>) -> Result<DataFrame, PolarsError> {
        let resolved: Vec<String> = cols
            .iter()
            .map(|c| self.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        let refs: Vec<&str> = resolved.iter().map(|s| s.as_str()).collect();
        let mut result = transformations::select(self, refs, self.case_sensitive)?;
        // When case-insensitive, PySpark returns column names in requested (e.g. lowercase) form.
        if !self.case_sensitive {
            for (requested, res) in cols.iter().zip(resolved.iter()) {
                if *requested != res.as_str() {
                    result = result.with_column_renamed(res, requested)?;
                }
            }
        }
        Ok(result)
    }

    /// Filter rows using a Polars expression.
    pub fn filter(&self, condition: Expr) -> Result<DataFrame, PolarsError> {
        transformations::filter(self, condition, self.case_sensitive)
    }

    /// Get a column reference by name (for building expressions).
    /// Respects case sensitivity: when false, "Age" resolves to column "age" if present.
    pub fn column(&self, name: &str) -> Result<Column, PolarsError> {
        let resolved = self.resolve_column_name(name)?;
        Ok(Column::new(resolved))
    }

    /// Add or replace a column. Use a [`Column`] (e.g. from `col("x")`, `rand(42)`, `randn(42)`).
    /// For `rand`/`randn`, generates one distinct value per row (PySpark-like).
    pub fn with_column(&self, column_name: &str, col: &Column) -> Result<DataFrame, PolarsError> {
        transformations::with_column(self, column_name, col, self.case_sensitive)
    }

    /// Add or replace a column using an expression. Prefer [`with_column`](Self::with_column) with a `Column` for rand/randn (per-row values).
    pub fn with_column_expr(
        &self,
        column_name: &str,
        expr: Expr,
    ) -> Result<DataFrame, PolarsError> {
        let col = Column::from_expr(expr, None);
        self.with_column(column_name, &col)
    }

    /// Group by columns (returns GroupedData for aggregation).
    /// Column names are resolved according to case sensitivity.
    pub fn group_by(&self, column_names: Vec<&str>) -> Result<GroupedData, PolarsError> {
        use polars::prelude::*;
        let resolved: Vec<String> = column_names
            .iter()
            .map(|c| self.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        let exprs: Vec<Expr> = resolved.iter().map(|name| col(name.as_str())).collect();
        let pl_df = self.df.as_ref().clone();
        let lazy_grouped = pl_df.clone().lazy().group_by(exprs);
        Ok(GroupedData {
            df: pl_df,
            lazy_grouped,
            grouping_cols: resolved,
            case_sensitive: self.case_sensitive,
        })
    }

    /// Cube: multiple grouping sets (all subsets of columns), then union (PySpark cube).
    pub fn cube(&self, column_names: Vec<&str>) -> Result<CubeRollupData, PolarsError> {
        let resolved: Vec<String> = column_names
            .iter()
            .map(|c| self.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(CubeRollupData {
            df: self.df.as_ref().clone(),
            grouping_cols: resolved,
            case_sensitive: self.case_sensitive,
            is_cube: true,
        })
    }

    /// Rollup: grouping sets (prefixes of columns), then union (PySpark rollup).
    pub fn rollup(&self, column_names: Vec<&str>) -> Result<CubeRollupData, PolarsError> {
        let resolved: Vec<String> = column_names
            .iter()
            .map(|c| self.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(CubeRollupData {
            df: self.df.as_ref().clone(),
            grouping_cols: resolved,
            case_sensitive: self.case_sensitive,
            is_cube: false,
        })
    }

    /// Join with another DataFrame on the given columns.
    /// Join column names are resolved on the left (and right must have matching names).
    pub fn join(
        &self,
        other: &DataFrame,
        on: Vec<&str>,
        how: JoinType,
    ) -> Result<DataFrame, PolarsError> {
        let resolved: Vec<String> = on
            .iter()
            .map(|c| self.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        let on_refs: Vec<&str> = resolved.iter().map(|s| s.as_str()).collect();
        join(self, other, on_refs, how, self.case_sensitive)
    }

    /// Order by columns (sort).
    /// Column names are resolved according to case sensitivity.
    pub fn order_by(
        &self,
        column_names: Vec<&str>,
        ascending: Vec<bool>,
    ) -> Result<DataFrame, PolarsError> {
        let resolved: Vec<String> = column_names
            .iter()
            .map(|c| self.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        let refs: Vec<&str> = resolved.iter().map(|s| s.as_str()).collect();
        transformations::order_by(self, refs, ascending, self.case_sensitive)
    }

    /// Order by sort expressions (asc/desc with nulls_first/last).
    pub fn order_by_exprs(&self, sort_orders: Vec<SortOrder>) -> Result<DataFrame, PolarsError> {
        transformations::order_by_exprs(self, sort_orders, self.case_sensitive)
    }

    /// Union (unionAll): stack another DataFrame vertically. Schemas must match (same columns, same order).
    pub fn union(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        transformations::union(self, other, self.case_sensitive)
    }

    /// Union by name: stack vertically, aligning columns by name.
    pub fn union_by_name(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        transformations::union_by_name(self, other, self.case_sensitive)
    }

    /// Distinct: drop duplicate rows (all columns or optional subset).
    pub fn distinct(&self, subset: Option<Vec<&str>>) -> Result<DataFrame, PolarsError> {
        transformations::distinct(self, subset, self.case_sensitive)
    }

    /// Drop one or more columns.
    pub fn drop(&self, columns: Vec<&str>) -> Result<DataFrame, PolarsError> {
        transformations::drop(self, columns, self.case_sensitive)
    }

    /// Drop rows with nulls (all columns or optional subset).
    pub fn dropna(&self, subset: Option<Vec<&str>>) -> Result<DataFrame, PolarsError> {
        transformations::dropna(self, subset, self.case_sensitive)
    }

    /// Fill nulls with a literal expression (applied to all columns).
    pub fn fillna(&self, value: Expr) -> Result<DataFrame, PolarsError> {
        transformations::fillna(self, value, self.case_sensitive)
    }

    /// Limit: return first n rows.
    pub fn limit(&self, n: usize) -> Result<DataFrame, PolarsError> {
        transformations::limit(self, n, self.case_sensitive)
    }

    /// Rename a column (old_name -> new_name).
    pub fn with_column_renamed(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<DataFrame, PolarsError> {
        transformations::with_column_renamed(self, old_name, new_name, self.case_sensitive)
    }

    /// Replace values in a column (old_value -> new_value). PySpark replace.
    pub fn replace(
        &self,
        column_name: &str,
        old_value: Expr,
        new_value: Expr,
    ) -> Result<DataFrame, PolarsError> {
        transformations::replace(self, column_name, old_value, new_value, self.case_sensitive)
    }

    /// Cross join with another DataFrame (cartesian product). PySpark crossJoin.
    pub fn cross_join(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        transformations::cross_join(self, other, self.case_sensitive)
    }

    /// Summary statistics. PySpark describe.
    pub fn describe(&self) -> Result<DataFrame, PolarsError> {
        transformations::describe(self, self.case_sensitive)
    }

    /// No-op: execution is eager by default. PySpark cache.
    pub fn cache(&self) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// No-op: execution is eager by default. PySpark persist.
    pub fn persist(&self) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// No-op. PySpark unpersist.
    pub fn unpersist(&self) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// Set difference: rows in self not in other. PySpark subtract / except.
    pub fn subtract(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        transformations::subtract(self, other, self.case_sensitive)
    }

    /// Set intersection: rows in both self and other. PySpark intersect.
    pub fn intersect(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        transformations::intersect(self, other, self.case_sensitive)
    }

    /// Sample a fraction of rows. PySpark sample(withReplacement, fraction, seed).
    pub fn sample(
        &self,
        with_replacement: bool,
        fraction: f64,
        seed: Option<u64>,
    ) -> Result<DataFrame, PolarsError> {
        transformations::sample(self, with_replacement, fraction, seed, self.case_sensitive)
    }

    /// Split into multiple DataFrames by weights. PySpark randomSplit(weights, seed).
    pub fn random_split(
        &self,
        weights: &[f64],
        seed: Option<u64>,
    ) -> Result<Vec<DataFrame>, PolarsError> {
        transformations::random_split(self, weights, seed, self.case_sensitive)
    }

    /// Stratified sample by column value. PySpark sampleBy(col, fractions, seed).
    /// fractions: list of (value as Expr, fraction) for that stratum.
    pub fn sample_by(
        &self,
        col_name: &str,
        fractions: &[(Expr, f64)],
        seed: Option<u64>,
    ) -> Result<DataFrame, PolarsError> {
        transformations::sample_by(self, col_name, fractions, seed, self.case_sensitive)
    }

    /// First row as a one-row DataFrame. PySpark first().
    pub fn first(&self) -> Result<DataFrame, PolarsError> {
        transformations::first(self, self.case_sensitive)
    }

    /// First n rows. PySpark head(n).
    pub fn head(&self, n: usize) -> Result<DataFrame, PolarsError> {
        transformations::head(self, n, self.case_sensitive)
    }

    /// Take first n rows. PySpark take(n).
    pub fn take(&self, n: usize) -> Result<DataFrame, PolarsError> {
        transformations::take(self, n, self.case_sensitive)
    }

    /// Last n rows. PySpark tail(n).
    pub fn tail(&self, n: usize) -> Result<DataFrame, PolarsError> {
        transformations::tail(self, n, self.case_sensitive)
    }

    /// True if the DataFrame has zero rows. PySpark isEmpty.
    pub fn is_empty(&self) -> bool {
        transformations::is_empty(self)
    }

    /// Rename columns. PySpark toDF(*colNames).
    pub fn to_df(&self, names: Vec<&str>) -> Result<DataFrame, PolarsError> {
        transformations::to_df(self, &names, self.case_sensitive)
    }

    /// Statistical helper. PySpark df.stat().cov / .corr.
    pub fn stat(&self) -> DataFrameStat<'_> {
        DataFrameStat { df: self }
    }

    /// Correlation matrix of all numeric columns. PySpark df.corr() returns a DataFrame of pairwise correlations.
    pub fn corr(&self) -> Result<DataFrame, PolarsError> {
        self.stat().corr_matrix()
    }

    /// Pearson correlation between two columns (scalar). PySpark df.corr(col1, col2).
    pub fn corr_cols(&self, col1: &str, col2: &str) -> Result<f64, PolarsError> {
        self.stat().corr(col1, col2)
    }

    /// Sample covariance between two columns (scalar). PySpark df.cov(col1, col2).
    pub fn cov_cols(&self, col1: &str, col2: &str) -> Result<f64, PolarsError> {
        self.stat().cov(col1, col2)
    }

    /// Summary statistics (alias for describe). PySpark summary.
    pub fn summary(&self) -> Result<DataFrame, PolarsError> {
        self.describe()
    }

    /// Collect rows as JSON strings (one per row). PySpark toJSON.
    pub fn to_json(&self) -> Result<Vec<String>, PolarsError> {
        transformations::to_json(self)
    }

    /// Return execution plan description. PySpark explain.
    pub fn explain(&self) -> String {
        transformations::explain(self)
    }

    /// Return schema as tree string. PySpark printSchema (returns string; print to stdout if needed).
    pub fn print_schema(&self) -> Result<String, PolarsError> {
        transformations::print_schema(self)
    }

    /// No-op: Polars backend is eager. PySpark checkpoint.
    pub fn checkpoint(&self) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// No-op: Polars backend is eager. PySpark localCheckpoint.
    pub fn local_checkpoint(&self) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// No-op: single partition in Polars. PySpark repartition(n).
    pub fn repartition(&self, _num_partitions: usize) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// No-op: Polars has no range partitioning. PySpark repartitionByRange(n, cols).
    pub fn repartition_by_range(
        &self,
        _num_partitions: usize,
        _cols: Vec<&str>,
    ) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// Column names and dtype strings. PySpark dtypes. Returns (name, dtype_string) per column.
    pub fn dtypes(&self) -> Result<Vec<(String, String)>, PolarsError> {
        let schema = self.df.schema();
        Ok(schema
            .iter_names_and_dtypes()
            .map(|(name, dtype)| (name.to_string(), format!("{dtype:?}")))
            .collect())
    }

    /// No-op: we don't model partitions. PySpark sortWithinPartitions. Same as orderBy for compatibility.
    pub fn sort_within_partitions(
        &self,
        _cols: &[crate::functions::SortOrder],
    ) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// No-op: single partition in Polars. PySpark coalesce(n).
    pub fn coalesce(&self, _num_partitions: usize) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// No-op. PySpark hint (query planner hint).
    pub fn hint(&self, _name: &str, _params: &[i32]) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// Returns true (eager single-node). PySpark isLocal.
    pub fn is_local(&self) -> bool {
        true
    }

    /// Returns empty vec (no file sources). PySpark inputFiles.
    pub fn input_files(&self) -> Vec<String> {
        Vec::new()
    }

    /// No-op; returns false. PySpark sameSemantics.
    pub fn same_semantics(&self, _other: &DataFrame) -> bool {
        false
    }

    /// No-op; returns 0. PySpark semanticHash.
    pub fn semantic_hash(&self) -> u64 {
        0
    }

    /// No-op. PySpark observe (metrics).
    pub fn observe(&self, _name: &str, _expr: Expr) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// No-op. PySpark withWatermark (streaming).
    pub fn with_watermark(
        &self,
        _event_time: &str,
        _delay: &str,
    ) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// Select by expression strings (minimal: column names, optionally "col as alias"). PySpark selectExpr.
    pub fn select_expr(&self, exprs: &[String]) -> Result<DataFrame, PolarsError> {
        transformations::select_expr(self, exprs, self.case_sensitive)
    }

    /// Select columns whose names match the regex. PySpark colRegex.
    pub fn col_regex(&self, pattern: &str) -> Result<DataFrame, PolarsError> {
        transformations::col_regex(self, pattern, self.case_sensitive)
    }

    /// Add or replace multiple columns. PySpark withColumns. Accepts `Column` so rand/randn get per-row values.
    pub fn with_columns(&self, exprs: &[(String, Column)]) -> Result<DataFrame, PolarsError> {
        transformations::with_columns(self, exprs, self.case_sensitive)
    }

    /// Rename multiple columns. PySpark withColumnsRenamed.
    pub fn with_columns_renamed(
        &self,
        renames: &[(String, String)],
    ) -> Result<DataFrame, PolarsError> {
        transformations::with_columns_renamed(self, renames, self.case_sensitive)
    }

    /// NA sub-API. PySpark df.na().
    pub fn na(&self) -> DataFrameNa<'_> {
        DataFrameNa { df: self }
    }

    /// Skip first n rows. PySpark offset(n).
    pub fn offset(&self, n: usize) -> Result<DataFrame, PolarsError> {
        transformations::offset(self, n, self.case_sensitive)
    }

    /// Transform by a function. PySpark transform(func).
    pub fn transform<F>(&self, f: F) -> Result<DataFrame, PolarsError>
    where
        F: FnOnce(DataFrame) -> Result<DataFrame, PolarsError>,
    {
        transformations::transform(self, f)
    }

    /// Frequent items. PySpark freqItems (stub).
    pub fn freq_items(&self, columns: &[&str], support: f64) -> Result<DataFrame, PolarsError> {
        transformations::freq_items(self, columns, support, self.case_sensitive)
    }

    /// Approximate quantiles. PySpark approxQuantile (stub).
    pub fn approx_quantile(
        &self,
        column: &str,
        probabilities: &[f64],
    ) -> Result<DataFrame, PolarsError> {
        transformations::approx_quantile(self, column, probabilities, self.case_sensitive)
    }

    /// Cross-tabulation. PySpark crosstab (stub).
    pub fn crosstab(&self, col1: &str, col2: &str) -> Result<DataFrame, PolarsError> {
        transformations::crosstab(self, col1, col2, self.case_sensitive)
    }

    /// Unpivot (melt). PySpark melt (stub).
    pub fn melt(&self, id_vars: &[&str], value_vars: &[&str]) -> Result<DataFrame, PolarsError> {
        transformations::melt(self, id_vars, value_vars, self.case_sensitive)
    }

    /// Pivot (wide format). PySpark pivot. Stub: not yet implemented; use crosstab for two-column count.
    pub fn pivot(
        &self,
        _pivot_col: &str,
        _values: Option<Vec<&str>>,
    ) -> Result<DataFrame, PolarsError> {
        Err(PolarsError::InvalidOperation(
            "pivot is not yet implemented; use crosstab(col1, col2) for two-column cross-tabulation."
                .into(),
        ))
    }

    /// Set difference keeping duplicates. PySpark exceptAll.
    pub fn except_all(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        transformations::except_all(self, other, self.case_sensitive)
    }

    /// Set intersection keeping duplicates. PySpark intersectAll.
    pub fn intersect_all(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        transformations::intersect_all(self, other, self.case_sensitive)
    }

    /// Write this DataFrame to a Delta table at the given path.
    /// Requires the `delta` feature. If `overwrite` is true, replaces the table; otherwise appends.
    #[cfg(feature = "delta")]
    pub fn write_delta(
        &self,
        path: impl AsRef<std::path::Path>,
        overwrite: bool,
    ) -> Result<(), PolarsError> {
        crate::delta::write_delta(self.df.as_ref(), path, overwrite)
    }

    /// Stub when `delta` feature is disabled.
    #[cfg(not(feature = "delta"))]
    pub fn write_delta(
        &self,
        _path: impl AsRef<std::path::Path>,
        _overwrite: bool,
    ) -> Result<(), PolarsError> {
        Err(PolarsError::InvalidOperation(
            "Delta Lake requires the 'delta' feature. Build with --features delta.".into(),
        ))
    }

    /// Register this DataFrame as an in-memory "delta table" by name (same namespace as saveAsTable). Readable via `read_delta(name)` or `table(name)`.
    pub fn save_as_delta_table(&self, session: &crate::session::SparkSession, name: &str) {
        session.register_table(name, self.clone());
    }

    /// Return a writer for generic format (parquet, csv, json). PySpark-style write API.
    pub fn write(&self) -> DataFrameWriter<'_> {
        DataFrameWriter {
            df: self,
            mode: WriteMode::Overwrite,
            format: WriteFormat::Parquet,
            options: HashMap::new(),
            partition_by: Vec::new(),
        }
    }
}

/// Write mode: overwrite or append (PySpark DataFrameWriter.mode for path-based save).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    Overwrite,
    Append,
}

/// Save mode for saveAsTable (PySpark default is ErrorIfExists).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SaveMode {
    /// Throw if table already exists (PySpark default).
    ErrorIfExists,
    /// Replace existing table.
    Overwrite,
    /// Append to existing table; create if not exists. Column names align.
    Append,
    /// No-op if table exists; create if not.
    Ignore,
}

/// Output format for generic write (PySpark DataFrameWriter.format).
#[derive(Clone, Copy)]
pub enum WriteFormat {
    Parquet,
    Csv,
    Json,
}

/// Builder for writing DataFrame to path (PySpark DataFrameWriter).
pub struct DataFrameWriter<'a> {
    df: &'a DataFrame,
    mode: WriteMode,
    format: WriteFormat,
    options: HashMap<String, String>,
    partition_by: Vec<String>,
}

impl<'a> DataFrameWriter<'a> {
    pub fn mode(mut self, mode: WriteMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn format(mut self, format: WriteFormat) -> Self {
        self.format = format;
        self
    }

    /// Add a single option (PySpark: option(key, value)). Returns self for chaining.
    pub fn option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    /// Add multiple options (PySpark: options(**kwargs)). Returns self for chaining.
    pub fn options(mut self, opts: impl IntoIterator<Item = (String, String)>) -> Self {
        for (k, v) in opts {
            self.options.insert(k, v);
        }
        self
    }

    /// Partition output by the given columns (PySpark: partitionBy(cols)).
    pub fn partition_by(mut self, cols: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.partition_by = cols.into_iter().map(|s| s.into()).collect();
        self
    }

    /// Save the DataFrame as a table (PySpark: saveAsTable). In-memory by default; when spark.sql.warehouse.dir is set, persists to disk for cross-session access.
    pub fn save_as_table(
        &self,
        session: &SparkSession,
        name: &str,
        mode: SaveMode,
    ) -> Result<(), PolarsError> {
        use polars::prelude::*;
        use std::fs;
        use std::path::Path;

        let warehouse_path = session.warehouse_dir().map(|w| Path::new(w).join(name));
        let warehouse_exists = warehouse_path.as_ref().is_some_and(|p| p.is_dir());

        fn persist_to_warehouse(
            df: &crate::dataframe::DataFrame,
            dir: &Path,
        ) -> Result<(), PolarsError> {
            use std::fs;
            fs::create_dir_all(dir).map_err(|e| {
                PolarsError::ComputeError(format!("saveAsTable: create dir: {e}").into())
            })?;
            let file_path = dir.join("data.parquet");
            df.write()
                .mode(crate::dataframe::WriteMode::Overwrite)
                .format(crate::dataframe::WriteFormat::Parquet)
                .save(&file_path)
        }

        let final_df = match mode {
            SaveMode::ErrorIfExists => {
                if session.saved_table_exists(name) || warehouse_exists {
                    return Err(PolarsError::InvalidOperation(
                        format!(
                            "Table or view '{name}' already exists. SaveMode is ErrorIfExists."
                        )
                        .into(),
                    ));
                }
                if let Some(ref p) = warehouse_path {
                    persist_to_warehouse(self.df, p)?;
                }
                self.df.clone()
            }
            SaveMode::Overwrite => {
                if let Some(ref p) = warehouse_path {
                    let _ = fs::remove_dir_all(p);
                    persist_to_warehouse(self.df, p)?;
                }
                self.df.clone()
            }
            SaveMode::Append => {
                let existing_pl = if let Some(existing) = session.get_saved_table(name) {
                    existing.df.as_ref().clone()
                } else if let (Some(ref p), true) = (warehouse_path.as_ref(), warehouse_exists) {
                    // Read from warehouse (data.parquet convention)
                    let data_file = p.join("data.parquet");
                    let read_path = if data_file.is_file() {
                        data_file.as_path()
                    } else {
                        p.as_ref()
                    };
                    let lf = LazyFrame::scan_parquet(read_path, ScanArgsParquet::default())
                        .map_err(|e| {
                            PolarsError::ComputeError(
                                format!("saveAsTable append: read warehouse: {e}").into(),
                            )
                        })?;
                    lf.collect().map_err(|e| {
                        PolarsError::ComputeError(
                            format!("saveAsTable append: collect: {e}").into(),
                        )
                    })?
                } else {
                    // New table
                    session.register_table(name, self.df.clone());
                    if let Some(ref p) = warehouse_path {
                        persist_to_warehouse(self.df, p)?;
                    }
                    return Ok(());
                };
                let new_pl = self.df.df.as_ref().clone();
                let existing_cols: Vec<&str> = existing_pl
                    .get_column_names()
                    .iter()
                    .map(|s| s.as_str())
                    .collect();
                let new_cols = new_pl.get_column_names();
                let missing: Vec<_> = existing_cols
                    .iter()
                    .filter(|c| !new_cols.iter().any(|n| n.as_str() == **c))
                    .collect();
                if !missing.is_empty() {
                    return Err(PolarsError::InvalidOperation(
                        format!(
                            "saveAsTable append: new DataFrame missing columns: {:?}",
                            missing
                        )
                        .into(),
                    ));
                }
                let new_ordered = new_pl.select(existing_cols.iter().copied())?;
                let mut combined = existing_pl;
                combined.vstack_mut(&new_ordered)?;
                let merged = crate::dataframe::DataFrame::from_polars_with_options(
                    combined,
                    self.df.case_sensitive,
                );
                if let Some(ref p) = warehouse_path {
                    let _ = fs::remove_dir_all(p);
                    persist_to_warehouse(&merged, p)?;
                }
                merged
            }
            SaveMode::Ignore => {
                if session.saved_table_exists(name) || warehouse_exists {
                    return Ok(());
                }
                if let Some(ref p) = warehouse_path {
                    persist_to_warehouse(self.df, p)?;
                }
                self.df.clone()
            }
        };
        session.register_table(name, final_df);
        Ok(())
    }

    /// Write as Parquet (PySpark: parquet(path)). Equivalent to format(Parquet).save(path).
    pub fn parquet(&self, path: impl AsRef<std::path::Path>) -> Result<(), PolarsError> {
        DataFrameWriter {
            df: self.df,
            mode: self.mode,
            format: WriteFormat::Parquet,
            options: self.options.clone(),
            partition_by: self.partition_by.clone(),
        }
        .save(path)
    }

    /// Write as CSV (PySpark: csv(path)). Equivalent to format(Csv).save(path).
    pub fn csv(&self, path: impl AsRef<std::path::Path>) -> Result<(), PolarsError> {
        DataFrameWriter {
            df: self.df,
            mode: self.mode,
            format: WriteFormat::Csv,
            options: self.options.clone(),
            partition_by: self.partition_by.clone(),
        }
        .save(path)
    }

    /// Write as JSON lines (PySpark: json(path)). Equivalent to format(Json).save(path).
    pub fn json(&self, path: impl AsRef<std::path::Path>) -> Result<(), PolarsError> {
        DataFrameWriter {
            df: self.df,
            mode: self.mode,
            format: WriteFormat::Json,
            options: self.options.clone(),
            partition_by: self.partition_by.clone(),
        }
        .save(path)
    }

    /// Write to path. Overwrite replaces; append reads existing (if any) and concatenates then writes.
    /// With partition_by, path is a directory; each partition is written as path/col1=val1/col2=val2/... with partition columns omitted from the file (Spark/Hive style).
    pub fn save(&self, path: impl AsRef<std::path::Path>) -> Result<(), PolarsError> {
        use polars::prelude::*;
        let path = path.as_ref();
        let to_write: PlDataFrame = match self.mode {
            WriteMode::Overwrite => self.df.df.as_ref().clone(),
            WriteMode::Append => {
                if self.partition_by.is_empty() {
                    let existing: Option<PlDataFrame> = if path.exists() && path.is_file() {
                        match self.format {
                            WriteFormat::Parquet => {
                                LazyFrame::scan_parquet(path, ScanArgsParquet::default())
                                    .and_then(|lf| lf.collect())
                                    .ok()
                            }
                            WriteFormat::Csv => LazyCsvReader::new(path)
                                .with_has_header(true)
                                .finish()
                                .and_then(|lf| lf.collect())
                                .ok(),
                            WriteFormat::Json => LazyJsonLineReader::new(path)
                                .finish()
                                .and_then(|lf| lf.collect())
                                .ok(),
                        }
                    } else {
                        None
                    };
                    match existing {
                        Some(existing) => {
                            let lfs: [LazyFrame; 2] =
                                [existing.lazy(), self.df.df.as_ref().clone().lazy()];
                            concat(lfs, UnionArgs::default())?.collect()?
                        }
                        None => self.df.df.as_ref().clone(),
                    }
                } else {
                    self.df.df.as_ref().clone()
                }
            }
        };

        if !self.partition_by.is_empty() {
            return self.save_partitioned(path, &to_write);
        }

        match self.format {
            WriteFormat::Parquet => {
                let mut file = std::fs::File::create(path).map_err(|e| {
                    PolarsError::ComputeError(format!("write parquet create: {e}").into())
                })?;
                let mut df_mut = to_write;
                ParquetWriter::new(&mut file)
                    .finish(&mut df_mut)
                    .map_err(|e| PolarsError::ComputeError(format!("write parquet: {e}").into()))?;
            }
            WriteFormat::Csv => {
                let has_header = self
                    .options
                    .get("header")
                    .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                    .unwrap_or(true);
                let delimiter = self
                    .options
                    .get("sep")
                    .and_then(|s| s.bytes().next())
                    .unwrap_or(b',');
                let mut file = std::fs::File::create(path).map_err(|e| {
                    PolarsError::ComputeError(format!("write csv create: {e}").into())
                })?;
                CsvWriter::new(&mut file)
                    .include_header(has_header)
                    .with_separator(delimiter)
                    .finish(&mut to_write.clone())
                    .map_err(|e| PolarsError::ComputeError(format!("write csv: {e}").into()))?;
            }
            WriteFormat::Json => {
                let mut file = std::fs::File::create(path).map_err(|e| {
                    PolarsError::ComputeError(format!("write json create: {e}").into())
                })?;
                JsonWriter::new(&mut file)
                    .finish(&mut to_write.clone())
                    .map_err(|e| PolarsError::ComputeError(format!("write json: {e}").into()))?;
            }
        }
        Ok(())
    }

    /// Write partitioned by columns: path/col1=val1/col2=val2/part-00000.{ext}. Partition columns are not written into the file (Spark/Hive style).
    fn save_partitioned(&self, path: &Path, to_write: &PlDataFrame) -> Result<(), PolarsError> {
        use polars::prelude::*;
        let resolved: Vec<String> = self
            .partition_by
            .iter()
            .map(|c| self.df.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        let all_names = to_write.get_column_names();
        let data_cols: Vec<&str> = all_names
            .iter()
            .filter(|n| !resolved.iter().any(|r| r == n.as_str()))
            .map(|n| n.as_str())
            .collect();

        let unique_keys = to_write
            .select(resolved.iter().map(|s| s.as_str()).collect::<Vec<_>>())?
            .unique::<Option<&[String]>, String>(
                None,
                polars::prelude::UniqueKeepStrategy::First,
                None,
            )?;

        if self.mode == WriteMode::Overwrite && path.exists() {
            if path.is_dir() {
                std::fs::remove_dir_all(path).map_err(|e| {
                    PolarsError::ComputeError(
                        format!("write partitioned: remove_dir_all: {e}").into(),
                    )
                })?;
            } else {
                std::fs::remove_file(path).map_err(|e| {
                    PolarsError::ComputeError(format!("write partitioned: remove_file: {e}").into())
                })?;
            }
        }
        std::fs::create_dir_all(path).map_err(|e| {
            PolarsError::ComputeError(format!("write partitioned: create_dir_all: {e}").into())
        })?;

        let ext = match self.format {
            WriteFormat::Parquet => "parquet",
            WriteFormat::Csv => "csv",
            WriteFormat::Json => "json",
        };

        for row_idx in 0..unique_keys.height() {
            let row = unique_keys
                .get(row_idx)
                .ok_or_else(|| PolarsError::ComputeError("partition_row: get row".into()))?;
            let filter_expr = partition_row_to_filter_expr(&resolved, &row)?;
            let subset = to_write.clone().lazy().filter(filter_expr).collect()?;
            let subset = subset.select(data_cols.iter().copied())?;
            if subset.height() == 0 {
                continue;
            }

            let part_path: std::path::PathBuf = resolved
                .iter()
                .zip(row.iter())
                .map(|(name, av)| format!("{}={}", name, format_partition_value(av)))
                .fold(path.to_path_buf(), |p, seg| p.join(seg));
            std::fs::create_dir_all(&part_path).map_err(|e| {
                PolarsError::ComputeError(
                    format!("write partitioned: create_dir_all partition: {e}").into(),
                )
            })?;

            let file_idx = if self.mode == WriteMode::Append {
                let suffix = format!(".{ext}");
                let max_n = std::fs::read_dir(&part_path)
                    .map(|rd| {
                        rd.filter_map(Result::ok)
                            .filter_map(|e| {
                                e.file_name().to_str().and_then(|s| {
                                    s.strip_prefix("part-")
                                        .and_then(|t| t.strip_suffix(&suffix))
                                        .and_then(|t| t.parse::<u32>().ok())
                                })
                            })
                            .max()
                            .unwrap_or(0)
                    })
                    .unwrap_or(0);
                max_n + 1
            } else {
                0
            };
            let filename = format!("part-{file_idx:05}.{ext}");
            let file_path = part_path.join(&filename);

            match self.format {
                WriteFormat::Parquet => {
                    let mut file = std::fs::File::create(&file_path).map_err(|e| {
                        PolarsError::ComputeError(
                            format!("write partitioned parquet create: {e}").into(),
                        )
                    })?;
                    let mut df_mut = subset;
                    ParquetWriter::new(&mut file)
                        .finish(&mut df_mut)
                        .map_err(|e| {
                            PolarsError::ComputeError(
                                format!("write partitioned parquet: {e}").into(),
                            )
                        })?;
                }
                WriteFormat::Csv => {
                    let has_header = self
                        .options
                        .get("header")
                        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                        .unwrap_or(true);
                    let delimiter = self
                        .options
                        .get("sep")
                        .and_then(|s| s.bytes().next())
                        .unwrap_or(b',');
                    let mut file = std::fs::File::create(&file_path).map_err(|e| {
                        PolarsError::ComputeError(
                            format!("write partitioned csv create: {e}").into(),
                        )
                    })?;
                    CsvWriter::new(&mut file)
                        .include_header(has_header)
                        .with_separator(delimiter)
                        .finish(&mut subset.clone())
                        .map_err(|e| {
                            PolarsError::ComputeError(format!("write partitioned csv: {e}").into())
                        })?;
                }
                WriteFormat::Json => {
                    let mut file = std::fs::File::create(&file_path).map_err(|e| {
                        PolarsError::ComputeError(
                            format!("write partitioned json create: {e}").into(),
                        )
                    })?;
                    JsonWriter::new(&mut file)
                        .finish(&mut subset.clone())
                        .map_err(|e| {
                            PolarsError::ComputeError(format!("write partitioned json: {e}").into())
                        })?;
                }
            }
        }
        Ok(())
    }
}

impl Clone for DataFrame {
    fn clone(&self) -> Self {
        DataFrame {
            df: self.df.clone(),
            case_sensitive: self.case_sensitive,
        }
    }
}

/// Format a partition column value for use in a directory name (Spark/Hive style).
/// Null becomes "__HIVE_DEFAULT_PARTITION__"; other values use string representation with path-unsafe chars replaced.
fn format_partition_value(av: &AnyValue<'_>) -> String {
    let s = match av {
        AnyValue::Null => "__HIVE_DEFAULT_PARTITION__".to_string(),
        AnyValue::Boolean(b) => b.to_string(),
        AnyValue::Int32(i) => i.to_string(),
        AnyValue::Int64(i) => i.to_string(),
        AnyValue::UInt32(u) => u.to_string(),
        AnyValue::UInt64(u) => u.to_string(),
        AnyValue::Float32(f) => f.to_string(),
        AnyValue::Float64(f) => f.to_string(),
        AnyValue::String(s) => s.to_string(),
        AnyValue::StringOwned(s) => s.as_str().to_string(),
        AnyValue::Date(d) => d.to_string(),
        _ => av.to_string(),
    };
    // Replace path separators and other unsafe chars so the value is a valid path segment
    s.replace([std::path::MAIN_SEPARATOR, '/'], "_")
}

/// Build a filter expression that matches rows where partition columns equal the given row values.
fn partition_row_to_filter_expr(
    col_names: &[String],
    row: &[AnyValue<'_>],
) -> Result<Expr, PolarsError> {
    if col_names.len() != row.len() {
        return Err(PolarsError::ComputeError(
            format!(
                "partition_row_to_filter_expr: {} columns but {} row values",
                col_names.len(),
                row.len()
            )
            .into(),
        ));
    }
    let mut pred = None::<Expr>;
    for (name, av) in col_names.iter().zip(row.iter()) {
        let clause = match av {
            AnyValue::Null => col(name.as_str()).is_null(),
            AnyValue::Boolean(b) => col(name.as_str()).eq(lit(*b)),
            AnyValue::Int32(i) => col(name.as_str()).eq(lit(*i)),
            AnyValue::Int64(i) => col(name.as_str()).eq(lit(*i)),
            AnyValue::UInt32(u) => col(name.as_str()).eq(lit(*u)),
            AnyValue::UInt64(u) => col(name.as_str()).eq(lit(*u)),
            AnyValue::Float32(f) => col(name.as_str()).eq(lit(*f)),
            AnyValue::Float64(f) => col(name.as_str()).eq(lit(*f)),
            AnyValue::String(s) => col(name.as_str()).eq(lit(s.to_string())),
            AnyValue::StringOwned(s) => col(name.as_str()).eq(lit(s.clone())),
            _ => {
                // Fallback: compare as string
                let s = av.to_string();
                col(name.as_str()).cast(DataType::String).eq(lit(s))
            }
        };
        pred = Some(match pred {
            None => clause,
            Some(p) => p.and(clause),
        });
    }
    Ok(pred.unwrap_or_else(|| lit(true)))
}

/// Convert Polars AnyValue to serde_json::Value for language bindings (Node, etc.).
fn any_value_to_json(av: AnyValue<'_>) -> JsonValue {
    match av {
        AnyValue::Null => JsonValue::Null,
        AnyValue::Boolean(b) => JsonValue::Bool(b),
        AnyValue::Int32(i) => JsonValue::Number(serde_json::Number::from(i)),
        AnyValue::Int64(i) => JsonValue::Number(serde_json::Number::from(i)),
        AnyValue::UInt32(u) => JsonValue::Number(serde_json::Number::from(u)),
        AnyValue::UInt64(u) => JsonValue::Number(serde_json::Number::from(u)),
        AnyValue::Float32(f) => serde_json::Number::from_f64(f64::from(f))
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        AnyValue::Float64(f) => serde_json::Number::from_f64(f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        AnyValue::String(s) => JsonValue::String(s.to_string()),
        AnyValue::StringOwned(s) => JsonValue::String(s.to_string()),
        _ => JsonValue::Null,
    }
}
