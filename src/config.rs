//! Configuration for robin-sparkless sessions.
//!
//! Use [`SparklessConfig`] to configure a session from code or environment variables,
//! then create a session with [`SparkSession::from_config`](crate::SparkSession::from_config).

use std::collections::HashMap;
use std::path::PathBuf;

/// Configuration for building a [`SparkSession`](crate::SparkSession).
///
/// Can be constructed manually or from environment variables via [`SparklessConfig::from_env`].
#[derive(Clone, Debug, Default)]
pub struct SparklessConfig {
    /// Directory for disk-backed tables (saveAsTable). Maps to `spark.sql.warehouse.dir`.
    pub warehouse_dir: Option<PathBuf>,
    /// Optional temp directory for intermediate data. Reserved for future use.
    pub temp_dir: Option<PathBuf>,
    /// When true, column names are case-sensitive. Default is false (PySpark default).
    /// Maps to `spark.sql.caseSensitive`.
    pub case_sensitive: bool,
    /// Extra Spark-style config key-value pairs (e.g. `spark.app.name`, `spark.executor.memory`).
    pub extra: HashMap<String, String>,
}

impl SparklessConfig {
    /// Build config from environment variables.
    ///
    /// - `ROBIN_SPARKLESS_WAREHOUSE_DIR` → `warehouse_dir`
    /// - `ROBIN_SPARKLESS_TEMP_DIR` → `temp_dir`
    /// - `ROBIN_SPARKLESS_CASE_SENSITIVE` → `case_sensitive` (any value that is "true" or "1" case-insensitively)
    /// - `ROBIN_SPARKLESS_CONFIG_*` → keys in `extra`; the key is the suffix after the prefix, with underscores converted to dots (e.g. `ROBIN_SPARKLESS_CONFIG_SPARK_APP_NAME` → `spark.app.name`)
    pub fn from_env() -> Self {
        let warehouse_dir = std::env::var("ROBIN_SPARKLESS_WAREHOUSE_DIR")
            .ok()
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);
        let temp_dir = std::env::var("ROBIN_SPARKLESS_TEMP_DIR")
            .ok()
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);
        let case_sensitive = std::env::var("ROBIN_SPARKLESS_CASE_SENSITIVE")
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);
        let prefix = "ROBIN_SPARKLESS_CONFIG_";
        let mut extra = HashMap::new();
        for (k, v) in std::env::vars() {
            if let Some(suffix) = k.strip_prefix(prefix) {
                let key = suffix.replace('_', ".");
                if !key.is_empty() && !v.is_empty() {
                    extra.insert(key, v);
                }
            }
        }
        SparklessConfig {
            warehouse_dir,
            temp_dir,
            case_sensitive,
            extra,
        }
    }

    /// Convert to the session config map (spark.sql.warehouse.dir, spark.sql.caseSensitive, plus extra).
    pub(crate) fn to_session_config(&self) -> HashMap<String, String> {
        let mut m = self.extra.clone();
        if let Some(ref d) = self.warehouse_dir {
            if let Some(s) = d.to_str() {
                m.insert("spark.sql.warehouse.dir".to_string(), s.to_string());
            }
        }
        m.insert(
            "spark.sql.caseSensitive".to_string(),
            if self.case_sensitive { "true" } else { "false" }.to_string(),
        );
        m
    }
}
