//! DataFrameReader for reading various file formats (CSV, Parquet, JSON, Delta).

use std::collections::HashMap;
use std::path::Path;

use polars::prelude::PolarsError;

use crate::dataframe::DataFrame;

use super::SparkSession;

/// DataFrameReader for reading various file formats
/// Similar to PySpark's DataFrameReader with option/options/format/load/table
pub struct DataFrameReader {
    pub(super) session: SparkSession,
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
            Some("delta") => self.session.read_delta_from_path(path),
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
        let path = path.as_ref();
        let path_display = path.display();
        let pl_path = PlRefPath::try_from_path(path).map_err(|e| {
            PolarsError::ComputeError(format!("csv({path_display}): path: {e}").into())
        })?;
        let reader = LazyCsvReader::new(pl_path);
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
        let path = path.as_ref();
        let pl_path = PlRefPath::try_from_path(path)
            .map_err(|e| PolarsError::ComputeError(format!("json: path: {e}").into()))?;
        let reader = LazyJsonLineReader::new(pl_path);
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
        self.session.read_delta_from_path(path)
    }
}
