use crate::dataframe::DataFrame;
use polars::prelude::{
    DataFrame as PlDataFrame, DataType, NamedFrom, PolarsError, Series, TimeUnit,
};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Builder for creating a SparkSession with configuration options
#[derive(Clone)]
pub struct SparkSessionBuilder {
    app_name: Option<String>,
    master: Option<String>,
    config: HashMap<String, String>,
}

impl Default for SparkSessionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSessionBuilder {
    pub fn new() -> Self {
        SparkSessionBuilder {
            app_name: None,
            master: None,
            config: HashMap::new(),
        }
    }

    pub fn app_name(mut self, name: impl Into<String>) -> Self {
        self.app_name = Some(name.into());
        self
    }

    pub fn master(mut self, master: impl Into<String>) -> Self {
        self.master = Some(master.into());
        self
    }

    pub fn config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.insert(key.into(), value.into());
        self
    }

    pub fn get_or_create(self) -> SparkSession {
        SparkSession::new(self.app_name, self.master, self.config)
    }
}

/// Catalog of temporary view names to DataFrames (session-scoped). Uses Arc<Mutex<>> for Send+Sync (Python bindings).
pub type TempViewCatalog = Arc<Mutex<HashMap<String, DataFrame>>>;

/// Main entry point for creating DataFrames and executing queries
/// Similar to PySpark's SparkSession but using Polars as the backend
#[derive(Clone)]
pub struct SparkSession {
    app_name: Option<String>,
    master: Option<String>,
    config: HashMap<String, String>,
    /// Temporary views: name -> DataFrame. Session-scoped; cleared when session is dropped.
    pub(crate) catalog: TempViewCatalog,
}

impl SparkSession {
    pub fn new(
        app_name: Option<String>,
        master: Option<String>,
        config: HashMap<String, String>,
    ) -> Self {
        SparkSession {
            app_name,
            master,
            config,
            catalog: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register a DataFrame as a temporary view (PySpark: createOrReplaceTempView).
    /// The view is session-scoped and is dropped when the session is dropped.
    pub fn create_or_replace_temp_view(&self, name: &str, df: DataFrame) {
        let _ = self
            .catalog
            .lock()
            .map(|mut m| m.insert(name.to_string(), df));
    }

    /// Global temp view (PySpark: createGlobalTempView). Stub: uses same catalog as temp view.
    pub fn create_global_temp_view(&self, name: &str, df: DataFrame) {
        self.create_or_replace_temp_view(name, df);
    }

    /// Global temp view (PySpark: createOrReplaceGlobalTempView). Stub: uses same catalog as temp view.
    pub fn create_or_replace_global_temp_view(&self, name: &str, df: DataFrame) {
        self.create_or_replace_temp_view(name, df);
    }

    /// Drop a temporary view by name (PySpark: catalog.dropTempView).
    /// No error if the view does not exist.
    pub fn drop_temp_view(&self, name: &str) {
        let _ = self.catalog.lock().map(|mut m| m.remove(name));
    }

    /// Drop a global temporary view (PySpark: catalog.dropGlobalTempView). Stub: same catalog as temp view.
    pub fn drop_global_temp_view(&self, name: &str) {
        self.drop_temp_view(name);
    }

    /// Check if a temporary view exists.
    pub fn table_exists(&self, name: &str) -> bool {
        self.catalog
            .lock()
            .map(|m| m.contains_key(name))
            .unwrap_or(false)
    }

    /// Return temporary view names in this session.
    pub fn list_temp_view_names(&self) -> Vec<String> {
        self.catalog
            .lock()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Look up a temporary view by name (PySpark: table(name)).
    /// Returns an error if the view does not exist.
    pub fn table(&self, name: &str) -> Result<DataFrame, PolarsError> {
        self.catalog
            .lock()
            .map_err(|_| PolarsError::InvalidOperation("catalog lock poisoned".into()))?
            .get(name)
            .cloned()
            .ok_or_else(|| {
                PolarsError::InvalidOperation(
                    format!(
                        "Table or view '{name}' not found. Register it with create_or_replace_temp_view."
                    )
                    .into(),
                )
            })
    }

    pub fn builder() -> SparkSessionBuilder {
        SparkSessionBuilder::new()
    }

    /// Return a reference to the session config (for catalog/conf compatibility).
    pub fn get_config(&self) -> &HashMap<String, String> {
        &self.config
    }

    /// Whether column names are case-sensitive (PySpark: spark.sql.caseSensitive).
    /// Default is false (case-insensitive matching).
    pub fn is_case_sensitive(&self) -> bool {
        self.config
            .get("spark.sql.caseSensitive")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    /// Create a DataFrame from a vector of tuples (i64, i64, String)
    ///
    /// # Example
    /// ```
    /// use robin_sparkless::session::SparkSession;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let spark = SparkSession::builder().app_name("test").get_or_create();
    /// let df = spark.create_dataframe(
    ///     vec![
    ///         (1, 25, "Alice".to_string()),
    ///         (2, 30, "Bob".to_string()),
    ///     ],
    ///     vec!["id", "age", "name"],
    /// )?;
    /// #     let _ = df;
    /// #     Ok(())
    /// # }
    /// ```
    pub fn create_dataframe(
        &self,
        data: Vec<(i64, i64, String)>,
        column_names: Vec<&str>,
    ) -> Result<DataFrame, PolarsError> {
        if column_names.len() != 3 {
            return Err(PolarsError::ComputeError(
                format!(
                    "create_dataframe: expected 3 column names for (i64, i64, String) tuples, got {}. Hint: provide exactly 3 names, e.g. [\"id\", \"age\", \"name\"].",
                    column_names.len()
                )
                .into(),
            ));
        }

        let mut cols: Vec<Series> = Vec::with_capacity(3);

        // First column: i64
        let col0: Vec<i64> = data.iter().map(|t| t.0).collect();
        cols.push(Series::new(column_names[0].into(), col0));

        // Second column: i64
        let col1: Vec<i64> = data.iter().map(|t| t.1).collect();
        cols.push(Series::new(column_names[1].into(), col1));

        // Third column: String
        let col2: Vec<String> = data.iter().map(|t| t.2.clone()).collect();
        cols.push(Series::new(column_names[2].into(), col2));

        let pl_df = PlDataFrame::new(cols.iter().map(|s| s.clone().into()).collect())?;
        Ok(DataFrame::from_polars_with_options(
            pl_df,
            self.is_case_sensitive(),
        ))
    }

    /// Create a DataFrame from a Polars DataFrame
    pub fn create_dataframe_from_polars(&self, df: PlDataFrame) -> DataFrame {
        DataFrame::from_polars_with_options(df, self.is_case_sensitive())
    }

    /// Create a DataFrame from rows and a schema (arbitrary column count and types).
    ///
    /// `rows`: each inner vec is one row; length must match schema length. Values are JSON-like (i64, f64, string, bool, null).
    /// `schema`: list of (column_name, dtype_string), e.g. `[("id", "bigint"), ("name", "string")]`.
    /// Supported dtype strings: bigint, int, long, double, float, string, str, varchar, boolean, bool, date, timestamp, datetime.
    pub fn create_dataframe_from_rows(
        &self,
        rows: Vec<Vec<JsonValue>>,
        schema: Vec<(String, String)>,
    ) -> Result<DataFrame, PolarsError> {
        if schema.is_empty() {
            return Err(PolarsError::InvalidOperation(
                "create_dataframe_from_rows: schema must not be empty".into(),
            ));
        }
        use chrono::{NaiveDate, NaiveDateTime};

        let mut cols: Vec<Series> = Vec::with_capacity(schema.len());

        for (col_idx, (name, type_str)) in schema.iter().enumerate() {
            let type_lower = type_str.trim().to_lowercase();
            let s = match type_lower.as_str() {
                "int" | "bigint" | "long" => {
                    let vals: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| {
                            let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                            match v {
                                JsonValue::Number(n) => n.as_i64(),
                                JsonValue::Null => None,
                                _ => None,
                            }
                        })
                        .collect();
                    Series::new(name.as_str().into(), vals)
                }
                "double" | "float" | "double_precision" => {
                    let vals: Vec<Option<f64>> = rows
                        .iter()
                        .map(|row| {
                            let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                            match v {
                                JsonValue::Number(n) => n.as_f64(),
                                JsonValue::Null => None,
                                _ => None,
                            }
                        })
                        .collect();
                    Series::new(name.as_str().into(), vals)
                }
                "string" | "str" | "varchar" => {
                    let vals: Vec<Option<String>> = rows
                        .iter()
                        .map(|row| {
                            let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                            match v {
                                JsonValue::String(s) => Some(s),
                                JsonValue::Null => None,
                                other => Some(other.to_string()),
                            }
                        })
                        .collect();
                    Series::new(name.as_str().into(), vals)
                }
                "boolean" | "bool" => {
                    let vals: Vec<Option<bool>> = rows
                        .iter()
                        .map(|row| {
                            let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                            match v {
                                JsonValue::Bool(b) => Some(b),
                                JsonValue::Null => None,
                                _ => None,
                            }
                        })
                        .collect();
                    Series::new(name.as_str().into(), vals)
                }
                "date" => {
                    let epoch = crate::date_utils::epoch_naive_date();
                    let vals: Vec<Option<i32>> = rows
                        .iter()
                        .map(|row| {
                            let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                            match v {
                                JsonValue::String(s) => NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                                    .ok()
                                    .map(|d| (d - epoch).num_days() as i32),
                                JsonValue::Null => None,
                                _ => None,
                            }
                        })
                        .collect();
                    let series = Series::new(name.as_str().into(), vals);
                    series
                        .cast(&DataType::Date)
                        .map_err(|e| PolarsError::ComputeError(format!("date cast: {e}").into()))?
                }
                "timestamp" | "datetime" | "timestamp_ntz" => {
                    let vals: Vec<Option<i64>> =
                        rows.iter()
                            .map(|row| {
                                let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                                match v {
                                    JsonValue::String(s) => {
                                        let parsed = NaiveDateTime::parse_from_str(
                                            &s,
                                            "%Y-%m-%dT%H:%M:%S%.f",
                                        )
                                        .or_else(|_| {
                                            NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S")
                                        })
                                        .or_else(|_| {
                                            NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                                                .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
                                        });
                                        parsed.ok().map(|dt| dt.and_utc().timestamp_micros())
                                    }
                                    JsonValue::Number(n) => n.as_i64(),
                                    JsonValue::Null => None,
                                    _ => None,
                                }
                            })
                            .collect();
                    let series = Series::new(name.as_str().into(), vals);
                    series
                        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
                        .map_err(|e| {
                            PolarsError::ComputeError(format!("datetime cast: {e}").into())
                        })?
                }
                _ => {
                    return Err(PolarsError::ComputeError(
                        format!(
                            "create_dataframe_from_rows: unsupported type '{type_str}' for column '{name}'"
                        )
                        .into(),
                    ));
                }
            };
            cols.push(s);
        }

        let pl_df = PlDataFrame::new(cols.iter().map(|s| s.clone().into()).collect())?;
        Ok(DataFrame::from_polars_with_options(
            pl_df,
            self.is_case_sensitive(),
        ))
    }

    /// Create a DataFrame with a single column `id` (bigint) containing values from start to end (exclusive) with step.
    /// PySpark: spark.range(end) or spark.range(start, end, step).
    ///
    /// - `range(end)` → 0 to end-1, step 1
    /// - `range(start, end)` → start to end-1, step 1
    /// - `range(start, end, step)` → start, start+step, ... up to but not including end
    pub fn range(&self, start: i64, end: i64, step: i64) -> Result<DataFrame, PolarsError> {
        if step == 0 {
            return Err(PolarsError::InvalidOperation(
                "range: step must not be 0".into(),
            ));
        }
        let mut vals: Vec<i64> = Vec::new();
        let mut v = start;
        if step > 0 {
            while v < end {
                vals.push(v);
                v = v.saturating_add(step);
            }
        } else {
            while v > end {
                vals.push(v);
                v = v.saturating_add(step);
            }
        }
        let col = Series::new("id".into(), vals);
        let pl_df = PlDataFrame::new(vec![col.into()])?;
        Ok(DataFrame::from_polars_with_options(
            pl_df,
            self.is_case_sensitive(),
        ))
    }

    /// Read a CSV file.
    ///
    /// Uses Polars' CSV reader with default options:
    /// - Header row is inferred (default: true)
    /// - Schema is inferred from first 100 rows
    ///
    /// # Example
    /// ```
    /// use robin_sparkless::SparkSession;
    ///
    /// let spark = SparkSession::builder().app_name("test").get_or_create();
    /// let df_result = spark.read_csv("data.csv");
    /// // Handle the Result as appropriate in your application
    /// ```
    pub fn read_csv(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let path = path.as_ref();
        let path_display = path.display();
        // Use LazyCsvReader - call finish() to get LazyFrame, then collect
        let lf = LazyCsvReader::new(path)
            .with_has_header(true)
            .with_infer_schema_length(Some(100))
            .finish()
            .map_err(|e| {
                PolarsError::ComputeError(
                    format!(
                        "read_csv({path_display}): {e} Hint: check that the file exists and is valid CSV."
                    )
                    .into(),
                )
            })?;
        let pl_df = lf.collect().map_err(|e| {
            PolarsError::ComputeError(
                format!("read_csv({path_display}): collect failed: {e}").into(),
            )
        })?;
        Ok(crate::dataframe::DataFrame::from_polars_with_options(
            pl_df,
            self.is_case_sensitive(),
        ))
    }

    /// Read a Parquet file.
    ///
    /// Uses Polars' Parquet reader. Parquet files have embedded schema, so
    /// schema inference is automatic.
    ///
    /// # Example
    /// ```
    /// use robin_sparkless::SparkSession;
    ///
    /// let spark = SparkSession::builder().app_name("test").get_or_create();
    /// let df_result = spark.read_parquet("data.parquet");
    /// // Handle the Result as appropriate in your application
    /// ```
    pub fn read_parquet(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let path = path.as_ref();
        // Use LazyFrame::scan_parquet
        let lf = LazyFrame::scan_parquet(path, ScanArgsParquet::default())?;
        let pl_df = lf.collect()?;
        Ok(crate::dataframe::DataFrame::from_polars_with_options(
            pl_df,
            self.is_case_sensitive(),
        ))
    }

    /// Read a JSON file (JSONL format - one JSON object per line).
    ///
    /// Uses Polars' JSONL reader with default options:
    /// - Schema is inferred from first 100 rows
    ///
    /// # Example
    /// ```
    /// use robin_sparkless::SparkSession;
    ///
    /// let spark = SparkSession::builder().app_name("test").get_or_create();
    /// let df_result = spark.read_json("data.json");
    /// // Handle the Result as appropriate in your application
    /// ```
    pub fn read_json(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        use std::num::NonZeroUsize;
        let path = path.as_ref();
        // Use LazyJsonLineReader - call finish() to get LazyFrame, then collect
        let lf = LazyJsonLineReader::new(path)
            .with_infer_schema_length(NonZeroUsize::new(100))
            .finish()?;
        let pl_df = lf.collect()?;
        Ok(crate::dataframe::DataFrame::from_polars_with_options(
            pl_df,
            self.is_case_sensitive(),
        ))
    }

    /// Execute a SQL query (SELECT only). Tables must be registered with `create_or_replace_temp_view`.
    /// Requires the `sql` feature. Supports: SELECT (columns or *), FROM (single table or JOIN),
    /// WHERE (basic predicates), GROUP BY + aggregates, ORDER BY, LIMIT.
    #[cfg(feature = "sql")]
    pub fn sql(&self, query: &str) -> Result<DataFrame, PolarsError> {
        crate::sql::execute_sql(self, query)
    }

    /// Execute a SQL query (stub when `sql` feature is disabled).
    #[cfg(not(feature = "sql"))]
    pub fn sql(&self, _query: &str) -> Result<DataFrame, PolarsError> {
        Err(PolarsError::InvalidOperation(
            "SQL queries require the 'sql' feature. Build with --features sql.".into(),
        ))
    }

    /// Read a Delta table at the given path (latest version).
    /// Requires the `delta` feature. Path can be local (e.g. `/tmp/table`) or `file:///...`.
    #[cfg(feature = "delta")]
    pub fn read_delta(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        crate::delta::read_delta(path, self.is_case_sensitive())
    }

    /// Read a Delta table at the given path, optionally at a specific version (time travel).
    #[cfg(feature = "delta")]
    pub fn read_delta_with_version(
        &self,
        path: impl AsRef<Path>,
        version: Option<i64>,
    ) -> Result<DataFrame, PolarsError> {
        crate::delta::read_delta_with_version(path, version, self.is_case_sensitive())
    }

    /// Stub when `delta` feature is disabled.
    #[cfg(not(feature = "delta"))]
    pub fn read_delta(&self, _path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        Err(PolarsError::InvalidOperation(
            "Delta Lake requires the 'delta' feature. Build with --features delta.".into(),
        ))
    }

    #[cfg(not(feature = "delta"))]
    pub fn read_delta_with_version(
        &self,
        _path: impl AsRef<Path>,
        _version: Option<i64>,
    ) -> Result<DataFrame, PolarsError> {
        Err(PolarsError::InvalidOperation(
            "Delta Lake requires the 'delta' feature. Build with --features delta.".into(),
        ))
    }

    /// Stop the session (cleanup resources)
    pub fn stop(&self) {
        // Cleanup if needed
    }
}

/// DataFrameReader for reading various file formats
/// Similar to PySpark's DataFrameReader with option/options/format/load/table
pub struct DataFrameReader {
    session: SparkSession,
    options: HashMap<String, String>,
    format: Option<String>,
}

impl DataFrameReader {
    pub fn new(session: SparkSession) -> Self {
        DataFrameReader {
            session,
            options: HashMap::new(),
            format: None,
        }
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

    /// Set the format for load() (PySpark: format("parquet") etc).
    pub fn format(mut self, fmt: impl Into<String>) -> Self {
        self.format = Some(fmt.into());
        self
    }

    /// Set the schema (PySpark: schema(schema)). Stub: stores but does not apply yet.
    pub fn schema(self, _schema: impl Into<String>) -> Self {
        self
    }

    /// Load data from path using format (or infer from extension) and options.
    pub fn load(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        let path = path.as_ref();
        let fmt = self.format.clone().or_else(|| {
            path.extension()
                .and_then(|e| e.to_str())
                .map(|s| s.to_lowercase())
        });
        match fmt.as_deref() {
            Some("parquet") => self.parquet(path),
            Some("csv") => self.csv(path),
            Some("json") | Some("jsonl") => self.json(path),
            #[cfg(feature = "delta")]
            Some("delta") => self.session.read_delta(path),
            _ => Err(PolarsError::ComputeError(
                format!(
                    "load: could not infer format for path '{}'. Use format('parquet'|'csv'|'json') before load.",
                    path.display()
                )
                .into(),
            )),
        }
    }

    /// Return the named table/view (PySpark: table(name)).
    pub fn table(&self, name: &str) -> Result<DataFrame, PolarsError> {
        self.session.table(name)
    }

    fn apply_csv_options(
        &self,
        reader: polars::prelude::LazyCsvReader,
    ) -> polars::prelude::LazyCsvReader {
        use polars::prelude::NullValues;
        let mut r = reader;
        if let Some(v) = self.options.get("header") {
            let has_header = v.eq_ignore_ascii_case("true") || v == "1";
            r = r.with_has_header(has_header);
        }
        if let Some(v) = self.options.get("inferSchema") {
            if v.eq_ignore_ascii_case("true") || v == "1" {
                let n = self
                    .options
                    .get("inferSchemaLength")
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(100);
                r = r.with_infer_schema_length(Some(n));
            }
        } else if let Some(v) = self.options.get("inferSchemaLength") {
            if let Ok(n) = v.parse::<usize>() {
                r = r.with_infer_schema_length(Some(n));
            }
        }
        if let Some(sep) = self.options.get("sep") {
            if let Some(b) = sep.bytes().next() {
                r = r.with_separator(b);
            }
        }
        if let Some(null_val) = self.options.get("nullValue") {
            r = r.with_null_values(Some(NullValues::AllColumnsSingle(null_val.clone().into())));
        }
        r
    }

    fn apply_json_options(
        &self,
        reader: polars::prelude::LazyJsonLineReader,
    ) -> polars::prelude::LazyJsonLineReader {
        use std::num::NonZeroUsize;
        let mut r = reader;
        if let Some(v) = self.options.get("inferSchemaLength") {
            if let Ok(n) = v.parse::<usize>() {
                r = r.with_infer_schema_length(NonZeroUsize::new(n));
            }
        }
        r
    }

    pub fn csv(&self, path: impl AsRef<std::path::Path>) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let path = path.as_ref();
        let path_display = path.display();
        let reader = LazyCsvReader::new(path);
        let reader = if self.options.is_empty() {
            reader
                .with_has_header(true)
                .with_infer_schema_length(Some(100))
        } else {
            self.apply_csv_options(
                reader
                    .with_has_header(true)
                    .with_infer_schema_length(Some(100)),
            )
        };
        let lf = reader.finish().map_err(|e| {
            PolarsError::ComputeError(format!("read csv({path_display}): {e}").into())
        })?;
        let pl_df = lf.collect().map_err(|e| {
            PolarsError::ComputeError(
                format!("read csv({path_display}): collect failed: {e}").into(),
            )
        })?;
        Ok(crate::dataframe::DataFrame::from_polars_with_options(
            pl_df,
            self.session.is_case_sensitive(),
        ))
    }

    pub fn parquet(&self, path: impl AsRef<std::path::Path>) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let path = path.as_ref();
        let lf = LazyFrame::scan_parquet(path, ScanArgsParquet::default())?;
        let pl_df = lf.collect()?;
        Ok(crate::dataframe::DataFrame::from_polars_with_options(
            pl_df,
            self.session.is_case_sensitive(),
        ))
    }

    pub fn json(&self, path: impl AsRef<std::path::Path>) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        use std::num::NonZeroUsize;
        let path = path.as_ref();
        let reader = LazyJsonLineReader::new(path);
        let reader = if self.options.is_empty() {
            reader.with_infer_schema_length(NonZeroUsize::new(100))
        } else {
            self.apply_json_options(reader.with_infer_schema_length(NonZeroUsize::new(100)))
        };
        let lf = reader.finish()?;
        let pl_df = lf.collect()?;
        Ok(crate::dataframe::DataFrame::from_polars_with_options(
            pl_df,
            self.session.is_case_sensitive(),
        ))
    }

    #[cfg(feature = "delta")]
    pub fn delta(&self, path: impl AsRef<std::path::Path>) -> Result<DataFrame, PolarsError> {
        self.session.read_delta(path)
    }
}

impl SparkSession {
    /// Get a DataFrameReader for reading files
    pub fn read(&self) -> DataFrameReader {
        DataFrameReader::new(SparkSession {
            app_name: self.app_name.clone(),
            master: self.master.clone(),
            config: self.config.clone(),
            catalog: self.catalog.clone(),
        })
    }
}

impl Default for SparkSession {
    fn default() -> Self {
        Self::builder().get_or_create()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spark_session_builder_basic() {
        let spark = SparkSession::builder().app_name("test_app").get_or_create();

        assert_eq!(spark.app_name, Some("test_app".to_string()));
    }

    #[test]
    fn test_spark_session_builder_with_master() {
        let spark = SparkSession::builder()
            .app_name("test_app")
            .master("local[*]")
            .get_or_create();

        assert_eq!(spark.app_name, Some("test_app".to_string()));
        assert_eq!(spark.master, Some("local[*]".to_string()));
    }

    #[test]
    fn test_spark_session_builder_with_config() {
        let spark = SparkSession::builder()
            .app_name("test_app")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "2g")
            .get_or_create();

        assert_eq!(
            spark.config.get("spark.executor.memory"),
            Some(&"4g".to_string())
        );
        assert_eq!(
            spark.config.get("spark.driver.memory"),
            Some(&"2g".to_string())
        );
    }

    #[test]
    fn test_spark_session_default() {
        let spark = SparkSession::default();
        assert!(spark.app_name.is_none());
        assert!(spark.master.is_none());
        assert!(spark.config.is_empty());
    }

    #[test]
    fn test_create_dataframe_success() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let data = vec![
            (1i64, 25i64, "Alice".to_string()),
            (2i64, 30i64, "Bob".to_string()),
        ];

        let result = spark.create_dataframe(data, vec!["id", "age", "name"]);

        assert!(result.is_ok());
        let df = result.unwrap();
        assert_eq!(df.count().unwrap(), 2);

        let columns = df.columns().unwrap();
        assert!(columns.contains(&"id".to_string()));
        assert!(columns.contains(&"age".to_string()));
        assert!(columns.contains(&"name".to_string()));
    }

    #[test]
    fn test_create_dataframe_wrong_column_count() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let data = vec![(1i64, 25i64, "Alice".to_string())];

        // Too few columns
        let result = spark.create_dataframe(data.clone(), vec!["id", "age"]);
        assert!(result.is_err());

        // Too many columns
        let result = spark.create_dataframe(data, vec!["id", "age", "name", "extra"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_dataframe_empty() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let data: Vec<(i64, i64, String)> = vec![];

        let result = spark.create_dataframe(data, vec!["id", "age", "name"]);

        assert!(result.is_ok());
        let df = result.unwrap();
        assert_eq!(df.count().unwrap(), 0);
    }

    #[test]
    fn test_create_dataframe_from_polars() {
        use polars::prelude::df;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let polars_df = df!(
            "x" => &[1, 2, 3],
            "y" => &[4, 5, 6]
        )
        .unwrap();

        let df = spark.create_dataframe_from_polars(polars_df);

        assert_eq!(df.count().unwrap(), 3);
        let columns = df.columns().unwrap();
        assert!(columns.contains(&"x".to_string()));
        assert!(columns.contains(&"y".to_string()));
    }

    #[test]
    fn test_read_csv_file_not_found() {
        let spark = SparkSession::builder().app_name("test").get_or_create();

        let result = spark.read_csv("nonexistent_file.csv");

        assert!(result.is_err());
    }

    #[test]
    fn test_read_parquet_file_not_found() {
        let spark = SparkSession::builder().app_name("test").get_or_create();

        let result = spark.read_parquet("nonexistent_file.parquet");

        assert!(result.is_err());
    }

    #[test]
    fn test_read_json_file_not_found() {
        let spark = SparkSession::builder().app_name("test").get_or_create();

        let result = spark.read_json("nonexistent_file.json");

        assert!(result.is_err());
    }

    #[test]
    fn test_sql_returns_error_without_feature_or_unknown_table() {
        let spark = SparkSession::builder().app_name("test").get_or_create();

        let result = spark.sql("SELECT * FROM table");

        assert!(result.is_err());
        match result {
            Err(PolarsError::InvalidOperation(msg)) => {
                let s = msg.to_string();
                // Without sql feature: "SQL queries require the 'sql' feature"
                // With sql feature but no table: "Table or view 'table' not found" or parse error
                assert!(
                    s.contains("SQL") || s.contains("Table") || s.contains("feature"),
                    "unexpected message: {s}"
                );
            }
            _ => panic!("Expected InvalidOperation error"),
        }
    }

    #[test]
    fn test_spark_session_stop() {
        let spark = SparkSession::builder().app_name("test").get_or_create();

        // stop() should complete without error
        spark.stop();
    }

    #[test]
    fn test_dataframe_reader_api() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let reader = spark.read();

        // All readers should return errors for non-existent files
        assert!(reader.csv("nonexistent.csv").is_err());
        assert!(reader.parquet("nonexistent.parquet").is_err());
        assert!(reader.json("nonexistent.json").is_err());
    }

    #[test]
    fn test_read_csv_with_valid_file() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let spark = SparkSession::builder().app_name("test").get_or_create();

        // Create a temporary CSV file
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "id,name,age").unwrap();
        writeln!(temp_file, "1,Alice,25").unwrap();
        writeln!(temp_file, "2,Bob,30").unwrap();
        temp_file.flush().unwrap();

        let result = spark.read_csv(temp_file.path());

        assert!(result.is_ok());
        let df = result.unwrap();
        assert_eq!(df.count().unwrap(), 2);

        let columns = df.columns().unwrap();
        assert!(columns.contains(&"id".to_string()));
        assert!(columns.contains(&"name".to_string()));
        assert!(columns.contains(&"age".to_string()));
    }

    #[test]
    fn test_read_json_with_valid_file() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let spark = SparkSession::builder().app_name("test").get_or_create();

        // Create a temporary JSONL file
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, r#"{{"id":1,"name":"Alice"}}"#).unwrap();
        writeln!(temp_file, r#"{{"id":2,"name":"Bob"}}"#).unwrap();
        temp_file.flush().unwrap();

        let result = spark.read_json(temp_file.path());

        assert!(result.is_ok());
        let df = result.unwrap();
        assert_eq!(df.count().unwrap(), 2);
    }

    #[test]
    fn test_read_csv_empty_file() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let spark = SparkSession::builder().app_name("test").get_or_create();

        // Create an empty CSV file (just header)
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "id,name").unwrap();
        temp_file.flush().unwrap();

        let result = spark.read_csv(temp_file.path());

        assert!(result.is_ok());
        let df = result.unwrap();
        assert_eq!(df.count().unwrap(), 0);
    }

    #[test]
    fn test_write_partitioned_parquet() {
        use crate::dataframe::{WriteFormat, WriteMode};
        use std::fs;
        use tempfile::TempDir;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![
                    (1, 25, "Alice".to_string()),
                    (2, 30, "Bob".to_string()),
                    (3, 25, "Carol".to_string()),
                ],
                vec!["id", "age", "name"],
            )
            .unwrap();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("out");
        df.write()
            .mode(WriteMode::Overwrite)
            .format(WriteFormat::Parquet)
            .partition_by(["age"])
            .save(&path)
            .unwrap();
        assert!(path.is_dir());
        let entries: Vec<_> = fs::read_dir(&path).unwrap().collect();
        assert_eq!(entries.len(), 2, "expected two partition dirs (age=25, age=30)");
        let names: Vec<String> = entries
            .iter()
            .filter_map(|e| e.as_ref().ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(names.iter().any(|n| n.starts_with("age=")));
        let df_read = spark.read_parquet(&path).unwrap();
        assert_eq!(df_read.count().unwrap(), 3);
    }
}
