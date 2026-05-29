//! PySpark 3.5 vs 4.0 JDBC type-mapping profile helpers (see docs/PYSPARK_4_PARITY_PLAN.md §5.4).

use crate::runtime_config::{PysparkCompat, SessionRuntimeConfig};

/// True when `compat=4.0` and legacy restore flags are off (PySpark 4 JDBC semantics).
pub fn jdbc_v4_type_mappings(cfg: &SessionRuntimeConfig) -> bool {
    cfg.pyspark_compat == PysparkCompat::V4_0
        && !cfg.legacy_postgres_datetime_mapping
        && !cfg.legacy_mysql_datetime_mapping
        && !cfg.legacy_oracle_timestamp_mapping
        && !cfg.legacy_mssqlserver_datetime_mapping
        && !cfg.legacy_db2_datetime_mapping
}

pub fn postgres_v4_datetime_mapping(cfg: &SessionRuntimeConfig) -> bool {
    cfg.pyspark_compat == PysparkCompat::V4_0 && !cfg.legacy_postgres_datetime_mapping
}

pub fn mysql_v4_type_mapping(cfg: &SessionRuntimeConfig) -> bool {
    cfg.pyspark_compat == PysparkCompat::V4_0 && !cfg.legacy_mysql_datetime_mapping
}

pub fn oracle_v4_timestamp_mapping(cfg: &SessionRuntimeConfig) -> bool {
    cfg.pyspark_compat == PysparkCompat::V4_0 && !cfg.legacy_oracle_timestamp_mapping
}

pub fn mssql_v4_type_mapping(cfg: &SessionRuntimeConfig) -> bool {
    cfg.pyspark_compat == PysparkCompat::V4_0 && !cfg.legacy_mssqlserver_datetime_mapping
}

pub fn db2_v4_type_mapping(cfg: &SessionRuntimeConfig) -> bool {
    cfg.pyspark_compat == PysparkCompat::V4_0 && !cfg.legacy_db2_datetime_mapping
}
