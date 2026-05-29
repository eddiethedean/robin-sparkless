//! Session runtime configuration for PySpark compatibility profiles (3.5 vs 4.0).

use std::collections::HashMap;

/// PySpark compatibility tier (see docs/PYSPARK_COMPAT_PROFILES.md).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum PysparkCompat {
    /// PySpark 3.2–3.5-like semantics (Sparkless default).
    #[default]
    V3_5,
    /// PySpark 4.0+ semantics when opted in.
    V4_0,
}

impl PysparkCompat {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "3.5" | "3" | "3.x" => Some(Self::V3_5),
            "4.0" | "4" | "4.x" => Some(Self::V4_0),
            _ => None,
        }
    }

    pub fn as_config_str(self) -> &'static str {
        match self {
            Self::V3_5 => "3.5",
            Self::V4_0 => "4.0",
        }
    }
}

/// Execution semantics derived from `spark.conf` and compat profile.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionRuntimeConfig {
    pub pyspark_compat: PysparkCompat,
    pub ansi_enabled: bool,
    pub disable_map_key_normalization: bool,
    pub infer_map_type_from_first_pair: bool,
    pub infer_array_type_from_first_element: bool,
    /// PySpark 3.5 JDBC datetime mapping for PostgreSQL (legacy restore).
    pub legacy_postgres_datetime_mapping: bool,
    /// PySpark 3.5 JDBC type mapping for MySQL/MariaDB (legacy restore).
    pub legacy_mysql_datetime_mapping: bool,
    /// PySpark 3.5 JDBC timestamp write mapping for Oracle (legacy restore).
    pub legacy_oracle_timestamp_mapping: bool,
    /// PySpark 3.5 JDBC type mapping for SQL Server (legacy restore).
    pub legacy_mssqlserver_datetime_mapping: bool,
    /// PySpark 3.5 JDBC type mapping for DB2 (legacy restore).
    pub legacy_db2_datetime_mapping: bool,
}

impl Default for SessionRuntimeConfig {
    fn default() -> Self {
        Self::for_compat(PysparkCompat::V3_5)
    }
}

fn parse_bool(s: &str) -> bool {
    matches!(s.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes")
}

fn bool_str(v: bool) -> String {
    if v {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

impl SessionRuntimeConfig {
    /// Bundle defaults for a compatibility profile.
    pub fn for_compat(compat: PysparkCompat) -> Self {
        match compat {
            PysparkCompat::V3_5 => Self {
                pyspark_compat: PysparkCompat::V3_5,
                ansi_enabled: false,
                disable_map_key_normalization: true,
                infer_map_type_from_first_pair: true,
                infer_array_type_from_first_element: true,
                legacy_postgres_datetime_mapping: true,
                legacy_mysql_datetime_mapping: true,
                legacy_oracle_timestamp_mapping: true,
                legacy_mssqlserver_datetime_mapping: true,
                legacy_db2_datetime_mapping: true,
            },
            PysparkCompat::V4_0 => Self {
                pyspark_compat: PysparkCompat::V4_0,
                ansi_enabled: true,
                disable_map_key_normalization: false,
                infer_map_type_from_first_pair: false,
                infer_array_type_from_first_element: false,
                legacy_postgres_datetime_mapping: false,
                legacy_mysql_datetime_mapping: false,
                legacy_oracle_timestamp_mapping: false,
                legacy_mssqlserver_datetime_mapping: false,
                legacy_db2_datetime_mapping: false,
            },
        }
    }

    /// Parse from session config map (and optional env compat before session is built).
    pub fn from_session_map(map: &HashMap<String, String>) -> Self {
        let mut cfg = map
            .get("sparkless.pyspark.compat")
            .and_then(|v| PysparkCompat::parse(v).map(Self::for_compat))
            .unwrap_or_default();

        if let Some(v) = map.get("spark.sql.ansi.enabled") {
            cfg.ansi_enabled = parse_bool(v);
        }
        if let Some(v) = map.get("spark.sql.legacy.disableMapKeyNormalization") {
            cfg.disable_map_key_normalization = parse_bool(v);
        }
        if let Some(v) = map.get("spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled") {
            cfg.infer_map_type_from_first_pair = parse_bool(v);
        }
        if let Some(v) = map.get("spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled")
        {
            cfg.infer_array_type_from_first_element = parse_bool(v);
        }
        if let Some(v) = map.get("spark.sql.legacy.postgres.datetimeMapping.enabled") {
            cfg.legacy_postgres_datetime_mapping = parse_bool(v);
        }
        if let Some(v) = map.get("spark.sql.legacy.mysql.datetimeMapping.enabled") {
            cfg.legacy_mysql_datetime_mapping = parse_bool(v);
        }
        if let Some(v) = map.get("spark.sql.legacy.oracle.timestampMapping.enabled") {
            cfg.legacy_oracle_timestamp_mapping = parse_bool(v);
        }
        if let Some(v) = map.get("spark.sql.legacy.mssqlserver.datetimeMapping.enabled") {
            cfg.legacy_mssqlserver_datetime_mapping = parse_bool(v);
        }
        if let Some(v) = map.get("spark.sql.legacy.db2.datetimeMapping.enabled") {
            cfg.legacy_db2_datetime_mapping = parse_bool(v);
        }
        cfg
    }

    /// Apply profile bundle keys into a session config map (used when setting compat).
    pub fn apply_profile_to_map(&self, map: &mut HashMap<String, String>) {
        map.insert(
            "sparkless.pyspark.compat".to_string(),
            self.pyspark_compat.as_config_str().to_string(),
        );
        map.insert("spark.sql.ansi.enabled".to_string(), bool_str(self.ansi_enabled));
        map.insert(
            "spark.sql.legacy.disableMapKeyNormalization".to_string(),
            bool_str(self.disable_map_key_normalization),
        );
        map.insert(
            "spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled".to_string(),
            bool_str(self.infer_map_type_from_first_pair),
        );
        map.insert(
            "spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled".to_string(),
            bool_str(self.infer_array_type_from_first_element),
        );
        map.insert(
            "spark.sql.legacy.postgres.datetimeMapping.enabled".to_string(),
            bool_str(self.legacy_postgres_datetime_mapping),
        );
        map.insert(
            "spark.sql.legacy.mysql.datetimeMapping.enabled".to_string(),
            bool_str(self.legacy_mysql_datetime_mapping),
        );
        map.insert(
            "spark.sql.legacy.oracle.timestampMapping.enabled".to_string(),
            bool_str(self.legacy_oracle_timestamp_mapping),
        );
        map.insert(
            "spark.sql.legacy.mssqlserver.datetimeMapping.enabled".to_string(),
            bool_str(self.legacy_mssqlserver_datetime_mapping),
        );
        map.insert(
            "spark.sql.legacy.db2.datetimeMapping.enabled".to_string(),
            bool_str(self.legacy_db2_datetime_mapping),
        );
    }

    /// Apply compat profile and write bundled keys into the map.
    pub fn set_compat_profile(map: &mut HashMap<String, String>, compat: PysparkCompat) {
        let cfg = Self::for_compat(compat);
        cfg.apply_profile_to_map(map);
    }

    /// Read compat from environment (SPARKLESS_PYSPARK_COMPAT).
    pub fn compat_from_env() -> Option<PysparkCompat> {
        std::env::var("SPARKLESS_PYSPARK_COMPAT")
            .ok()
            .and_then(|v| PysparkCompat::parse(&v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_v3_5_ansi_off() {
        let c = SessionRuntimeConfig::default();
        assert_eq!(c.pyspark_compat, PysparkCompat::V3_5);
        assert!(!c.ansi_enabled);
        assert!(c.disable_map_key_normalization);
    }

    #[test]
    fn v4_profile_ansi_on() {
        let c = SessionRuntimeConfig::for_compat(PysparkCompat::V4_0);
        assert!(c.ansi_enabled);
        assert!(!c.disable_map_key_normalization);
    }

    #[test]
    fn from_session_map_compat_4() {
        let mut m = HashMap::new();
        m.insert("sparkless.pyspark.compat".to_string(), "4.0".to_string());
        let c = SessionRuntimeConfig::from_session_map(&m);
        assert_eq!(c.pyspark_compat, PysparkCompat::V4_0);
        assert!(c.ansi_enabled);
    }

    #[test]
    fn ansi_override_after_profile() {
        let mut m = HashMap::new();
        m.insert("sparkless.pyspark.compat".to_string(), "4.0".to_string());
        m.insert("spark.sql.ansi.enabled".to_string(), "false".to_string());
        let c = SessionRuntimeConfig::from_session_map(&m);
        assert!(!c.ansi_enabled);
    }

    #[test]
    fn v4_jdbc_legacy_flags_off_by_default() {
        let c = SessionRuntimeConfig::for_compat(PysparkCompat::V4_0);
        assert!(!c.legacy_postgres_datetime_mapping);
        assert!(!c.legacy_mysql_datetime_mapping);
        assert!(!c.legacy_mssqlserver_datetime_mapping);
    }

    #[test]
    fn v3_jdbc_legacy_flags_on_by_default() {
        let c = SessionRuntimeConfig::for_compat(PysparkCompat::V3_5);
        assert!(c.legacy_postgres_datetime_mapping);
        assert!(c.legacy_mysql_datetime_mapping);
    }
}
