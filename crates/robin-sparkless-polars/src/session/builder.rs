//! Builder for creating a SparkSession with configuration options.

use std::collections::HashMap;

use robin_sparkless_core::SparklessConfig;

use super::{SparkSession, set_thread_udf_session};

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
        let session = SparkSession::new(self.app_name, self.master, self.config);
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
