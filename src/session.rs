//! Root-owned Session API; delegates to robin-sparkless-polars for execution.

use crate::EngineError;
use robin_sparkless_core::SparklessConfig;
use robin_sparkless_polars::{
    DataFrameReader as PolarsDataFrameReader, PlDataFrame, PolarsError,
    SparkSession as PolarsSparkSession, SparkSessionBuilder as PolarsSparkSessionBuilder,
};
use std::collections::HashMap;
use std::path::Path;

use crate::dataframe::DataFrame;

/// Root-owned SparkSession; delegates to the Polars backend.
#[derive(Clone)]
pub struct SparkSession(pub(crate) PolarsSparkSession);

/// Root-owned SparkSessionBuilder; delegates to the Polars backend.
#[derive(Clone)]
pub struct SparkSessionBuilder(pub(crate) PolarsSparkSessionBuilder);

/// Root-owned DataFrameReader; delegates to the Polars backend.
pub struct DataFrameReader(PolarsDataFrameReader);

impl SparkSessionBuilder {
    pub fn new() -> Self {
        SparkSessionBuilder(PolarsSparkSessionBuilder::new())
    }

    pub fn app_name(self, name: impl Into<String>) -> Self {
        SparkSessionBuilder(self.0.app_name(name))
    }

    pub fn master(self, master: impl Into<String>) -> Self {
        SparkSessionBuilder(self.0.master(master))
    }

    pub fn config(self, key: impl Into<String>, value: impl Into<String>) -> Self {
        SparkSessionBuilder(self.0.config(key, value))
    }

    /// Config key-value pairs set on the builder (for singleton compatibility check).
    pub fn get_config(&self) -> &HashMap<String, String> {
        self.0.get_config()
    }

    pub fn get_or_create(self) -> SparkSession {
        SparkSession(self.0.get_or_create())
    }

    pub fn with_config(self, config: &SparklessConfig) -> Self {
        SparkSessionBuilder(self.0.with_config(config))
    }
}

impl Default for SparkSessionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSession {
    pub fn builder() -> SparkSessionBuilder {
        SparkSessionBuilder(PolarsSparkSession::builder())
    }

    pub fn from_config(config: &SparklessConfig) -> SparkSession {
        SparkSession(PolarsSparkSession::from_config(config))
    }

    pub fn read(&self) -> DataFrameReader {
        DataFrameReader(PolarsDataFrameReader::new(self.0.clone()))
    }

    pub fn create_or_replace_temp_view(&self, name: &str, df: DataFrame) {
        self.0.create_or_replace_temp_view(name, df.0)
    }

    pub fn create_global_temp_view(&self, name: &str, df: DataFrame) {
        self.0.create_global_temp_view(name, df.0)
    }

    pub fn create_or_replace_global_temp_view(&self, name: &str, df: DataFrame) {
        self.0.create_or_replace_global_temp_view(name, df.0)
    }

    pub fn drop_temp_view(&self, name: &str) -> bool {
        self.0.drop_temp_view(name)
    }

    pub fn drop_global_temp_view(&self, name: &str) -> bool {
        self.0.drop_global_temp_view(name)
    }

    pub fn register_table(&self, name: &str, df: DataFrame) {
        self.0.register_table(name, df.0)
    }

    pub fn register_database(&self, name: &str) {
        self.0.register_database(name)
    }

    pub fn list_database_names(&self) -> Vec<String> {
        self.0.list_database_names()
    }

    pub fn database_exists(&self, name: &str) -> bool {
        self.0.database_exists(name)
    }

    pub fn get_saved_table(&self, name: &str) -> Option<DataFrame> {
        self.0.get_saved_table(name).map(DataFrame)
    }

    pub fn saved_table_exists(&self, name: &str) -> bool {
        self.0.saved_table_exists(name)
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.0.table_exists(name)
    }

    pub fn list_global_temp_view_names(&self) -> Vec<String> {
        self.0.list_global_temp_view_names()
    }

    pub fn list_temp_view_names(&self) -> Vec<String> {
        self.0.list_temp_view_names()
    }

    pub fn list_table_names(&self) -> Vec<String> {
        self.0.list_table_names()
    }

    pub fn app_name(&self) -> Option<String> {
        self.0.app_name()
    }

    pub fn new_session(&self) -> SparkSession {
        SparkSession(self.0.new_session())
    }

    pub fn current_database(&self) -> String {
        self.0.current_database()
    }

    pub fn set_current_database(&self, name: &str) -> Result<(), EngineError> {
        self.0.set_current_database(name)
    }

    pub fn cache_table(&self, name: &str) {
        self.0.cache_table(name)
    }

    pub fn uncache_table(&self, name: &str) {
        self.0.uncache_table(name)
    }

    pub fn is_cached(&self, name: &str) -> bool {
        self.0.is_cached(name)
    }

    pub fn drop_table(&self, name: &str) -> bool {
        self.0.drop_table(name)
    }

    pub fn drop_database(&self, name: &str) -> bool {
        self.0.drop_database(name)
    }

    pub fn warehouse_dir(&self) -> Option<&str> {
        self.0.warehouse_dir()
    }

    pub fn table(&self, name: &str) -> Result<DataFrame, PolarsError> {
        self.0.table(name).map(DataFrame)
    }

    pub fn get_config(&self) -> &HashMap<String, String> {
        self.0.get_config()
    }

    pub fn set_config(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.0.set_config(key, value);
    }

    pub fn is_case_sensitive(&self) -> bool {
        self.0.is_case_sensitive()
    }

    pub fn register_udf<F>(&self, name: &str, f: F) -> Result<(), PolarsError>
    where
        F: Fn(
                &[robin_sparkless_polars::Series],
            ) -> Result<robin_sparkless_polars::Series, PolarsError>
            + Send
            + Sync
            + 'static,
    {
        self.0.register_udf(name, f)
    }

    pub fn create_dataframe(
        &self,
        data: Vec<(i64, i64, String)>,
        column_names: Vec<&str>,
    ) -> Result<DataFrame, PolarsError> {
        self.0.create_dataframe(data, column_names).map(DataFrame)
    }

    pub fn create_dataframe_engine(
        &self,
        data: Vec<(i64, i64, String)>,
        column_names: Vec<&str>,
    ) -> Result<DataFrame, EngineError> {
        self.0
            .create_dataframe_engine(data, column_names)
            .map(DataFrame)
    }

    pub fn create_dataframe_from_polars(&self, df: PlDataFrame) -> DataFrame {
        DataFrame(self.0.create_dataframe_from_polars(df))
    }

    pub fn create_dataframe_from_rows(
        &self,
        rows: Vec<Vec<serde_json::Value>>,
        schema: Vec<(String, String)>,
        verify_schema: bool,
        schema_was_inferred: bool,
    ) -> Result<DataFrame, PolarsError> {
        self.0
            .create_dataframe_from_rows(rows, schema, verify_schema, schema_was_inferred)
            .map(DataFrame)
    }

    pub fn create_dataframe_from_rows_engine(
        &self,
        rows: Vec<Vec<serde_json::Value>>,
        schema: Vec<(String, String)>,
        verify_schema: bool,
        schema_was_inferred: bool,
    ) -> Result<DataFrame, EngineError> {
        self.0
            .create_dataframe_from_rows_engine(rows, schema, verify_schema, schema_was_inferred)
            .map(DataFrame)
    }

    /// #419: Create a DataFrame with a single column "value" from scalar values (e.g. createDataFrame([1,2,3], "bigint")).
    pub fn create_dataframe_from_single_column(
        &self,
        values: Vec<serde_json::Value>,
        type_str: &str,
    ) -> Result<DataFrame, PolarsError> {
        self.0
            .create_dataframe_from_single_column(values, type_str)
            .map(DataFrame)
    }

    pub fn range(&self, start: i64, end: i64, step: i64) -> Result<DataFrame, PolarsError> {
        self.0.range(start, end, step).map(DataFrame)
    }

    pub fn read_csv(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        self.0.read_csv(path).map(DataFrame)
    }

    pub fn read_csv_engine(&self, path: impl AsRef<Path>) -> Result<DataFrame, EngineError> {
        self.0.read_csv_engine(path).map(DataFrame)
    }

    pub fn read_parquet(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        self.0.read_parquet(path).map(DataFrame)
    }

    pub fn read_parquet_engine(&self, path: impl AsRef<Path>) -> Result<DataFrame, EngineError> {
        self.0.read_parquet_engine(path).map(DataFrame)
    }

    pub fn read_json(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        self.0.read_json(path).map(DataFrame)
    }

    pub fn read_json_engine(&self, path: impl AsRef<Path>) -> Result<DataFrame, EngineError> {
        self.0.read_json_engine(path).map(DataFrame)
    }

    pub fn sql(&self, query: &str) -> Result<DataFrame, PolarsError> {
        self.0.sql(query).map(DataFrame)
    }

    pub fn table_engine(&self, name: &str) -> Result<DataFrame, EngineError> {
        self.0.table_engine(name).map(DataFrame)
    }

    #[cfg(feature = "delta")]
    pub fn read_delta_path(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        self.0.read_delta_path(path).map(DataFrame)
    }

    pub fn read_delta_from_path(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        self.0.read_delta_from_path(path).map(DataFrame)
    }

    #[cfg(feature = "delta")]
    pub fn read_delta_path_with_version(
        &self,
        path: impl AsRef<Path>,
        version: Option<i64>,
    ) -> Result<DataFrame, PolarsError> {
        self.0
            .read_delta_path_with_version(path, version)
            .map(DataFrame)
    }

    #[cfg(feature = "delta")]
    pub fn read_delta(&self, name_or_path: &str) -> Result<DataFrame, PolarsError> {
        self.0.read_delta(name_or_path).map(DataFrame)
    }

    #[cfg(feature = "delta")]
    pub fn read_delta_with_version(
        &self,
        name_or_path: &str,
        version: Option<i64>,
    ) -> Result<DataFrame, PolarsError> {
        self.0
            .read_delta_with_version(name_or_path, version)
            .map(DataFrame)
    }

    pub fn stop(&self) {
        self.0.stop()
    }

    /// Get the UDF registry. Used internally for thread context management.
    pub fn udf_registry(&self) -> &robin_sparkless_polars::UdfRegistry {
        self.0.udf_registry()
    }
}

impl DataFrameReader {
    pub fn option(self, key: impl Into<String>, value: impl Into<String>) -> Self {
        DataFrameReader(self.0.option(key, value))
    }

    pub fn options(self, opts: impl IntoIterator<Item = (String, String)>) -> Self {
        DataFrameReader(self.0.options(opts))
    }

    pub fn format(self, fmt: impl Into<String>) -> Self {
        DataFrameReader(self.0.format(fmt))
    }

    pub fn schema(self, schema: impl Into<String>) -> Self {
        DataFrameReader(self.0.schema(schema))
    }

    pub fn load(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        self.0.load(path).map(DataFrame)
    }

    pub fn table(&self, name: &str) -> Result<DataFrame, PolarsError> {
        self.0.table(name).map(DataFrame)
    }

    pub fn csv(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        self.0.csv(path).map(DataFrame)
    }

    pub fn parquet(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        self.0.parquet(path).map(DataFrame)
    }

    pub fn json(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        self.0.json(path).map(DataFrame)
    }

    #[cfg(feature = "delta")]
    pub fn delta(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        self.0.delta(path).map(DataFrame)
    }

    /// JDBC read: load a table from an external database (e.g. PostgreSQL).
    /// Requires the `jdbc` or `sqlite` feature. Mirror of PySpark's spark.read.jdbc(url, table, properties).
    #[cfg(any(
        feature = "jdbc",
        feature = "jdbc_mysql",
        feature = "jdbc_mariadb",
        feature = "jdbc_mssql",
        feature = "jdbc_oracle",
        feature = "jdbc_db2",
        feature = "sqlite"
    ))]
    pub fn jdbc(
        &self,
        url: &str,
        table: &str,
        properties: &HashMap<String, String>,
    ) -> Result<DataFrame, PolarsError> {
        self.0
            .jdbc_with_properties(url, table, properties)
            .map(DataFrame)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
    }
}
