//! Builder for creating a SparkSession with configuration options.

use std::collections::HashMap;

use robin_sparkless_core::{SessionRuntimeConfig, SparklessConfig};

use super::{SharedCatalog, SparkSession, set_thread_udf_session};

/// Builder for creating a SparkSession with configuration options
#[derive(Clone)]
pub struct SparkSessionBuilder {
    app_name: Option<String>,
    master: Option<String>,
    config: HashMap<String, String>,
    shared_catalog: Option<SharedCatalog>,
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
            shared_catalog: None,
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

    /// Attach new sessions created by this builder to an explicitly shared application catalog.
    /// The shared catalog contains saved tables/databases; temporary views and session settings
    /// remain isolated between sessions.
    pub fn shared_catalog(mut self, catalog: SharedCatalog) -> Self {
        self.shared_catalog = Some(catalog);
        self
    }

    /// Config key-value pairs set on the builder (for singleton compatibility check).
    pub fn get_config(&self) -> &HashMap<String, String> {
        &self.config
    }

    pub fn get_or_create(self) -> SparkSession {
        let mut config = self.config;
        // Batch 8 / #789: spark.conf().get("spark.app.name") should return the app name.
        if let Some(name) = &self.app_name {
            config.insert("spark.app.name".to_string(), name.clone());
        }
        if !config.contains_key("sparkless.pyspark.compat")
            && let Some(compat) = SessionRuntimeConfig::compat_from_env()
        {
            SessionRuntimeConfig::set_compat_profile(&mut config, compat);
        }
        let session = SparkSession::new(self.app_name, self.master, config);
        let session = match self.shared_catalog {
            Some(catalog) => session.with_shared_catalog(catalog),
            None => session,
        };
        set_thread_udf_session(session.clone());
        session
    }

    /// Apply configuration from a [`SparklessConfig`](SparklessConfig).
    /// Merges warehouse dir, case sensitivity, and extra keys into the builder config.
    pub fn with_config(mut self, config: &SparklessConfig) -> Self {
        for (k, v) in config.to_session_config() {
            self.config.insert(k, v);
        }
        self
    }
}
