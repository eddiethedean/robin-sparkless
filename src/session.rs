use crate::dataframe::DataFrame;
use polars::prelude::{DataFrame as PlDataFrame, NamedFrom, PolarsError, Series};
use std::collections::HashMap;
use std::path::Path;

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

/// Main entry point for creating DataFrames and executing queries
/// Similar to PySpark's SparkSession but using Polars as the backend
#[derive(Clone)]
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
        Ok(DataFrame::from_polars_with_options(
            pl_df,
            self.is_case_sensitive(),
        ))
    }

    /// Create a DataFrame from a Polars DataFrame
    pub fn create_dataframe_from_polars(&self, df: PlDataFrame) -> DataFrame {
        DataFrame::from_polars_with_options(df, self.is_case_sensitive())
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
        // Use LazyCsvReader - call finish() to get LazyFrame, then collect
        let lf = LazyCsvReader::new(path)
            .with_has_header(true)
            .with_infer_schema_length(Some(100))
            .finish()?;
        let pl_df = lf.collect()?;
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
    fn test_sql_not_implemented() {
        let spark = SparkSession::builder().app_name("test").get_or_create();

        let result = spark.sql("SELECT * FROM table");

        assert!(result.is_err());
        match result {
            Err(PolarsError::InvalidOperation(msg)) => {
                assert!(msg.to_string().contains("not yet implemented"));
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
}
