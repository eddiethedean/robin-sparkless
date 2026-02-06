//! DataFrame module: main tabular type and submodules for transformations, aggregations, joins, stats.

mod aggregations;
mod joins;
mod stats;
mod transformations;

pub use aggregations::{CubeRollupData, GroupedData};
pub use joins::{join, JoinType};
pub use stats::DataFrameStat;
pub use transformations::{filter, order_by, order_by_exprs, select, with_column, DataFrameNa};

use crate::column::Column;
use crate::functions::SortOrder;
use crate::schema::StructType;
use polars::prelude::{
    AnyValue, DataFrame as PlDataFrame, Expr, PolarsError, SchemaNamesAndDtypes,
};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
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
    /// Column names are resolved according to case sensitivity.
    pub fn select(&self, cols: Vec<&str>) -> Result<DataFrame, PolarsError> {
        let resolved: Vec<String> = cols
            .iter()
            .map(|c| self.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        let refs: Vec<&str> = resolved.iter().map(|s| s.as_str()).collect();
        transformations::select(self, refs, self.case_sensitive)
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
        let lazy_grouped = self.df.as_ref().clone().lazy().group_by(exprs);
        Ok(GroupedData {
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

    /// Return a writer for generic format (parquet, csv, json). PySpark-style write API.
    pub fn write(&self) -> DataFrameWriter<'_> {
        DataFrameWriter {
            df: self,
            mode: WriteMode::Overwrite,
            format: WriteFormat::Parquet,
        }
    }
}

/// Write mode: overwrite or append (PySpark DataFrameWriter.mode).
#[derive(Clone, Copy)]
pub enum WriteMode {
    Overwrite,
    Append,
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

    /// Write to path. Overwrite replaces; append reads existing (if any) and concatenates then writes.
    pub fn save(&self, path: impl AsRef<std::path::Path>) -> Result<(), PolarsError> {
        use polars::prelude::*;
        let path = path.as_ref();
        let to_write: PlDataFrame = match self.mode {
            WriteMode::Overwrite => self.df.df.as_ref().clone(),
            WriteMode::Append => {
                use polars::prelude::*;
                let existing: Option<PlDataFrame> = if path.exists() {
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
                        let lfs: [polars::prelude::LazyFrame; 2] =
                            [existing.lazy(), self.df.df.as_ref().clone().lazy()];
                        polars::prelude::concat(lfs, UnionArgs::default())?.collect()?
                    }
                    None => self.df.df.as_ref().clone(),
                }
            }
        };
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
                let mut file = std::fs::File::create(path).map_err(|e| {
                    PolarsError::ComputeError(format!("write csv create: {e}").into())
                })?;
                CsvWriter::new(&mut file)
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
}

impl Clone for DataFrame {
    fn clone(&self) -> Self {
        DataFrame {
            df: self.df.clone(),
            case_sensitive: self.case_sensitive,
        }
    }
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
