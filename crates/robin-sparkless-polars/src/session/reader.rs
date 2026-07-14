//! DataFrameReader for reading various file formats (CSV, Parquet, JSON, Delta).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use polars::prelude::{PolarsError, SchemaRef};
use robin_sparkless_core::{DataType, StructField, StructType};

use crate::dataframe::DataFrame;
#[cfg(any(
    feature = "jdbc",
    feature = "jdbc_mysql",
    feature = "jdbc_mariadb",
    feature = "jdbc_mssql",
    feature = "jdbc_oracle",
    feature = "jdbc_db2",
    feature = "sqlite"
))]
use crate::jdbc::JdbcOptions;
use crate::schema::schema_from_json;
use crate::schema_conv::StructTypePolarsExt;

use super::{SparkSession, confine_io_path};

/// DataFrameReader for reading various file formats
/// Similar to PySpark's DataFrameReader with option/options/format/load/table
pub struct DataFrameReader {
    pub(super) session: SparkSession,
    options: HashMap<String, String>,
    format: Option<String>,
    /// Optional schema override from [`Self::schema`] (JSON StructType or Spark-like DDL).
    schema: Option<String>,
}

impl DataFrameReader {
    pub fn new(session: SparkSession) -> Self {
        DataFrameReader {
            session,
            options: HashMap::new(),
            format: None,
            schema: None,
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

    /// Set the schema used when reading CSV/JSON (PySpark: `schema(...)`).
    ///
    /// Accepts either:
    /// - JSON for [`StructType`] (`{"fields":[{"name":"id","data_type":"Long","nullable":true},...]}`)
    /// - Simple Spark-style DDL (`id LONG, name STRING`)
    ///
    /// Applied on [`Self::csv`] / [`Self::json`] / [`Self::load`] for those formats.
    /// Unsupported formats ignore an unset schema; if a schema is set for an unsupported
    /// format (e.g. parquet path via `load`), [`Self::load`] returns an error.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    fn parsed_schema(&self) -> Result<Option<StructType>, PolarsError> {
        match &self.schema {
            None => Ok(None),
            Some(s) if s.trim().is_empty() => Ok(None),
            Some(s) => parse_reader_schema(s).map(Some),
        }
    }

    fn polars_schema_ref(&self) -> Result<Option<SchemaRef>, PolarsError> {
        Ok(self
            .parsed_schema()?
            .map(|st| Arc::new(st.to_polars_schema()) as SchemaRef))
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
            Some("parquet") => {
                if self.schema.is_some() {
                    return Err(PolarsError::ComputeError(
                        "DataFrameReader.schema() is not applied for parquet; use CSV/JSON or cast after read"
                            .into(),
                    ));
                }
                self.parquet(path)
            }
            Some("csv") => self.csv(path),
            Some("json") | Some("jsonl") => self.json(path),
            #[cfg(feature = "delta")]
            Some("delta") => {
                if self.schema.is_some() {
                    return Err(PolarsError::ComputeError(
                        "DataFrameReader.schema() is not applied for delta; cast after read".into(),
                    ));
                }
                self.session.read_delta_from_path(path)
            }
            #[cfg(any(
                feature = "jdbc",
                feature = "jdbc_mysql",
                feature = "jdbc_mariadb",
                feature = "jdbc_mssql",
                feature = "jdbc_oracle",
                feature = "jdbc_db2",
                feature = "sqlite"
            ))]
            Some("jdbc") => {
                if self.schema.is_some() {
                    return Err(PolarsError::ComputeError(
                        "DataFrameReader.schema() is not applied for jdbc; cast after read".into(),
                    ));
                }
                let opts = JdbcOptions::from_options_map(&self.options).map_err(|e| {
                    PolarsError::ComputeError(format!("jdbc load: invalid options: {e}").into())
                })?;
                let pl_df = crate::jdbc::read_jdbc_to_polars(&opts).map_err(|e| {
                    PolarsError::ComputeError(format!("jdbc load: {e}").into())
                })?;
                Ok(DataFrame::from_polars_with_options(
                    pl_df,
                    self.session.is_case_sensitive(),
                ))
            }
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

    /// JDBC convenience: read using explicit url, dbtable, and properties map.
    ///
    /// This mirrors PySpark's `spark.read.jdbc(url, table, properties)` shape
    /// but is currently only used internally by higher-level APIs.
    #[cfg(any(
        feature = "jdbc",
        feature = "jdbc_mysql",
        feature = "jdbc_mariadb",
        feature = "jdbc_mssql",
        feature = "jdbc_oracle",
        feature = "jdbc_db2",
        feature = "sqlite"
    ))]
    pub fn jdbc_with_properties(
        &self,
        url: &str,
        table: &str,
        properties: &HashMap<String, String>,
    ) -> Result<DataFrame, crate::error::EngineError> {
        let opts = JdbcOptions::from_url_dbtable_and_properties(
            url.to_string(),
            table.to_string(),
            properties,
        )?;
        let pl_df = crate::jdbc::read_jdbc_to_polars(&opts)?;
        Ok(DataFrame::from_polars_with_options(
            pl_df,
            self.session.is_case_sensitive(),
        ))
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
            } else {
                // inferSchema=false: do not infer types (PySpark parity #543)
                r = r.with_infer_schema_length(Some(0));
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
        let path: PathBuf = confine_io_path(path.as_ref())?;
        let path = path.as_path();
        let path_display = path.display();
        let pl_path = PlRefPath::try_from_path(path).map_err(|e| {
            PolarsError::ComputeError(format!("csv({path_display}): path: {e}").into())
        })?;
        let schema = self.polars_schema_ref()?;
        let reader = LazyCsvReader::new(pl_path);
        let mut reader = if self.options.is_empty() {
            reader
                .with_has_header(true)
                // Default: inferSchema enabled with a reasonable sample size (PySpark parity tests rely on this).
                .with_infer_schema_length(Some(100))
        } else {
            self.apply_csv_options(
                reader
                    .with_has_header(true)
                    // When options are present but inferSchema/inferSchemaLength are not,
                    // keep the same default inference length as the no-options path.
                    .with_infer_schema_length(Some(100)),
            )
        };
        if let Some(schema) = schema {
            // Explicit schema: stop inferring and force dtypes (PySpark schema(...) parity).
            reader = reader
                .with_schema(Some(schema))
                .with_infer_schema_length(Some(0));
        }
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
        if self.schema.is_some() {
            return Err(PolarsError::ComputeError(
                "DataFrameReader.schema() is not applied for parquet; cast after read".into(),
            ));
        }
        let path: PathBuf = confine_io_path(path.as_ref())?;
        let path = path.as_path();
        let pl_path = PlRefPath::try_from_path(path)
            .map_err(|e| PolarsError::ComputeError(format!("parquet: path: {e}").into()))?;
        let lf = LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())?;
        let pl_df = lf.collect()?;
        Ok(crate::dataframe::DataFrame::from_polars_with_options(
            pl_df,
            self.session.is_case_sensitive(),
        ))
    }

    pub fn json(&self, path: impl AsRef<std::path::Path>) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        use std::num::NonZeroUsize;
        let path: PathBuf = confine_io_path(path.as_ref())?;
        let path = path.as_path();
        let pl_path = PlRefPath::try_from_path(path)
            .map_err(|e| PolarsError::ComputeError(format!("json: path: {e}").into()))?;
        let schema = self.polars_schema_ref()?;
        let reader = LazyJsonLineReader::new(pl_path);
        let mut reader = if self.options.is_empty() {
            reader.with_infer_schema_length(NonZeroUsize::new(100))
        } else {
            self.apply_json_options(reader.with_infer_schema_length(NonZeroUsize::new(100)))
        };
        if let Some(schema) = schema {
            // Force dtypes; still allow header discovery when needed.
            reader = reader.with_schema(Some(schema));
        }
        let lf = reader.finish()?;
        let pl_df = lf.collect()?;
        Ok(crate::dataframe::DataFrame::from_polars_with_options(
            pl_df,
            self.session.is_case_sensitive(),
        ))
    }

    #[cfg(feature = "delta")]
    pub fn delta(&self, path: impl AsRef<std::path::Path>) -> Result<DataFrame, PolarsError> {
        if self.schema.is_some() {
            return Err(PolarsError::ComputeError(
                "DataFrameReader.schema() is not applied for delta; cast after read".into(),
            ));
        }
        self.session.read_delta_from_path(path)
    }
}

/// Parse reader schema from JSON [`StructType`] or a simple Spark-style DDL string.
fn parse_reader_schema(raw: &str) -> Result<StructType, PolarsError> {
    let trimmed = raw.trim();
    if trimmed.starts_with('{') {
        return schema_from_json(trimmed).map_err(|e| {
            PolarsError::ComputeError(format!("DataFrameReader.schema JSON: {e}").into())
        });
    }
    parse_simple_spark_ddl(trimmed).map_err(|e| {
        PolarsError::ComputeError(
            format!(
                "DataFrameReader.schema(): expected JSON StructType or Spark-like DDL \
                 (e.g. `id LONG, name STRING`); {e}"
            )
            .into(),
        )
    })
}

fn parse_simple_spark_ddl(ddl: &str) -> Result<StructType, String> {
    let mut fields = Vec::new();
    for part in ddl.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let mut tokens = part.split_whitespace();
        let name = tokens
            .next()
            .ok_or_else(|| format!("missing column name in '{part}'"))?
            .to_string();
        let ty_token = tokens
            .next()
            .ok_or_else(|| format!("missing type for column '{name}'"))?;
        if tokens.next().is_some() {
            return Err(format!(
                "unsupported DDL fragment '{part}' (nested types/NULLABLE not supported in this parser)"
            ));
        }
        let data_type = spark_type_token_to_data_type(ty_token)
            .ok_or_else(|| format!("unsupported type '{ty_token}' for column '{name}'"))?;
        fields.push(StructField::new(name, data_type, true));
    }
    if fields.is_empty() {
        return Err("empty schema".to_string());
    }
    Ok(StructType::new(fields))
}

fn spark_type_token_to_data_type(token: &str) -> Option<DataType> {
    match token.to_ascii_lowercase().as_str() {
        "string" | "str" | "varchar" | "text" => Some(DataType::String),
        "int" | "integer" => Some(DataType::Integer),
        "long" | "bigint" => Some(DataType::Long),
        "double" | "float" | "real" => Some(DataType::Double),
        "boolean" | "bool" => Some(DataType::Boolean),
        "date" => Some(DataType::Date),
        "timestamp" | "datetime" => Some(DataType::Timestamp),
        "binary" | "bytes" => Some(DataType::Binary),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn parse_simple_ddl_types() {
        let st = parse_simple_spark_ddl("id LONG, name STRING, active BOOLEAN").unwrap();
        assert_eq!(st.fields().len(), 3);
        assert!(matches!(st.fields()[0].data_type, DataType::Long));
        assert!(matches!(st.fields()[1].data_type, DataType::String));
        assert!(matches!(st.fields()[2].data_type, DataType::Boolean));
    }

    #[test]
    fn csv_schema_override_forces_long() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.csv");
        {
            let mut f = std::fs::File::create(&path).unwrap();
            writeln!(f, "id,name").unwrap();
            writeln!(f, "1,a").unwrap();
            writeln!(f, "2,b").unwrap();
        }
        let spark = SparkSession::builder()
            .app_name("schema-test")
            .get_or_create();
        let df = spark
            .read()
            .schema("id LONG, name STRING")
            .csv(&path)
            .unwrap();
        let schema = df.schema().unwrap();
        assert!(matches!(schema.fields()[0].data_type, DataType::Long));
        assert!(matches!(schema.fields()[1].data_type, DataType::String));
    }
}
