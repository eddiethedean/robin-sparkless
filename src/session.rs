use polars::prelude::{DataFrame as PlDataFrame, LazyCsvReader, LazyFrame, LazyJsonLineReader, NamedFrom, PolarsError, ScanArgsParquet, Series};
use std::collections::HashMap;
use std::path::Path;
use crate::dataframe::DataFrame;

/// Builder for creating a SparkSession with configuration options
pub struct SparkSessionBuilder {
    app_name: Option<String>,
    master: Option<String>,
    config: HashMap<String, String>,
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

/// Main entry point for creating DataFrames and executing queries
/// Similar to PySpark's SparkSession but using Polars as the backend
pub struct SparkSession {
    app_name: Option<String>,
    master: Option<String>,
    config: HashMap<String, String>,
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
        }
    }

    pub fn builder() -> SparkSessionBuilder {
        SparkSessionBuilder::new()
    }

    /// Create a DataFrame from a vector of tuples (i64, i64, String)
    /// 
    /// # Example
    /// ```
    /// use robin_sparkless::session::SparkSession;
    /// 
    /// let spark = SparkSession::builder().app_name("test").get_or_create();
    /// let df = spark.create_dataframe(vec![
    ///     (1, 25, "Alice".to_string()),
    ///     (2, 30, "Bob".to_string()),
    /// ], vec!["id", "age", "name"])?;
    /// ```
    pub fn create_dataframe(
        &self,
        data: Vec<(i64, i64, String)>,
        column_names: Vec<&str>,
    ) -> Result<DataFrame, PolarsError> {
        if column_names.len() != 3 {
            return Err(PolarsError::ComputeError(
                format!(
                    "Expected 3 column names for (i64, i64, String) tuples, got {}",
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
        Ok(DataFrame::from_polars(pl_df))
    }

    /// Create a DataFrame from a Polars DataFrame
    pub fn create_dataframe_from_polars(&self, df: PlDataFrame) -> DataFrame {
        DataFrame::from_polars(df)
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
    /// let df = spark.read_csv("data.csv")?;
    /// ```
    pub fn read_csv(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let path = path.as_ref();
        // Use LazyCsvReader - call finish() to get LazyFrame, then collect
        let lf = LazyCsvReader::new(path)
            .with_has_header(true)
            .with_infer_schema_length(Some(100))
            .finish()?;
        let pl_df = lf.collect()?;
        Ok(crate::dataframe::DataFrame::from_polars(pl_df))
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
    /// let df = spark.read_parquet("data.parquet")?;
    /// ```
    pub fn read_parquet(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let path = path.as_ref();
        // Use LazyFrame::scan_parquet
        let lf = LazyFrame::scan_parquet(path, ScanArgsParquet::default())?;
        let pl_df = lf.collect()?;
        Ok(crate::dataframe::DataFrame::from_polars(pl_df))
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
    /// let df = spark.read_json("data.json")?;
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
        Ok(crate::dataframe::DataFrame::from_polars(pl_df))
    }

    /// Execute a SQL query (placeholder - Polars doesn't have built-in SQL)
    /// This would require integrating a SQL parser or using DataFusion's SQL support
    pub fn sql(&self, _query: &str) -> Result<DataFrame, PolarsError> {
        // TODO: Implement SQL execution
        // This could use Polars' expression system or integrate with a SQL parser
        Err(PolarsError::InvalidOperation(
            "SQL queries not yet implemented".into(),
        ))
    }

    /// Stop the session (cleanup resources)
    pub fn stop(&self) {
        // Cleanup if needed
    }
}

/// DataFrameReader for reading various file formats
/// Similar to PySpark's DataFrameReader
pub struct DataFrameReader {
    session: SparkSession,
}

impl DataFrameReader {
    pub fn new(session: SparkSession) -> Self {
        DataFrameReader { session }
    }

    pub fn csv(&self, path: impl AsRef<std::path::Path>) -> Result<DataFrame, PolarsError> {
        self.session.read_csv(path)
    }

    pub fn parquet(&self, path: impl AsRef<std::path::Path>) -> Result<DataFrame, PolarsError> {
        self.session.read_parquet(path)
    }

    pub fn json(&self, path: impl AsRef<std::path::Path>) -> Result<DataFrame, PolarsError> {
        self.session.read_json(path)
    }
}

impl SparkSession {
    /// Get a DataFrameReader for reading files
    pub fn read(&self) -> DataFrameReader {
        DataFrameReader::new(SparkSession {
            app_name: self.app_name.clone(),
            master: self.master.clone(),
            config: self.config.clone(),
        })
    }
}

impl Default for SparkSession {
    fn default() -> Self {
        Self::builder().get_or_create()
    }
}
