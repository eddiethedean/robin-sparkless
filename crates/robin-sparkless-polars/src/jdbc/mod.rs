//! Thin Polars-engine integration for JDBC storage drivers.
//!
//! Driver-neutral option parsing and SQL identifier policy live in
//! `robin-sparkless-jdbc`; concrete database connections and Polars conversion live in
//! `robin-sparkless-jdbc-polars`.

#[cfg(test)]
use std::collections::HashMap;

use crate::error::EngineError;

#[cfg(any(
    feature = "jdbc",
    feature = "sqlite",
    feature = "jdbc_mysql",
    feature = "jdbc_mariadb",
    feature = "jdbc_mssql",
    feature = "jdbc_oracle",
    feature = "jdbc_db2"
))]
use polars::prelude::DataFrame as PlDataFrame;

pub use robin_sparkless_jdbc::sql_ident;

pub use robin_sparkless_jdbc::{
    JdbcOptions, is_db2_url, is_mariadb_url, is_mssql_url, is_mysql_url, is_oracle_url,
    is_postgres_url, is_sqlite_url, jdbc_allow_arbitrary_sql,
};

#[cfg(test)]
fn driver_hint_is(driver: &Option<String>, expected: &str) -> bool {
    robin_sparkless_jdbc::driver_hint_matches(driver.as_deref(), expected)
}

/// Write a Polars frame through the configured JDBC driver.
#[cfg(any(
    feature = "jdbc",
    feature = "sqlite",
    feature = "jdbc_mysql",
    feature = "jdbc_mariadb",
    feature = "jdbc_mssql",
    feature = "jdbc_oracle",
    feature = "jdbc_db2"
))]
pub fn write_jdbc_from_polars(
    frame: &PlDataFrame,
    options: &JdbcOptions,
    mode: crate::dataframe::SaveMode,
) -> Result<(), EngineError> {
    let runtime = crate::udf_context::get_thread_runtime_config();
    let mode = match mode {
        crate::dataframe::SaveMode::ErrorIfExists => {
            robin_sparkless_jdbc_polars::JdbcSaveMode::ErrorIfExists
        }
        crate::dataframe::SaveMode::Overwrite => {
            robin_sparkless_jdbc_polars::JdbcSaveMode::Overwrite
        }
        crate::dataframe::SaveMode::Append => robin_sparkless_jdbc_polars::JdbcSaveMode::Append,
        crate::dataframe::SaveMode::Ignore => robin_sparkless_jdbc_polars::JdbcSaveMode::Ignore,
    };
    robin_sparkless_jdbc_polars::write_jdbc_from_polars(frame, options, mode, &runtime)
}

/// Read a JDBC source into a Polars frame using the active session settings.
#[cfg(any(
    feature = "jdbc",
    feature = "sqlite",
    feature = "jdbc_mysql",
    feature = "jdbc_mariadb",
    feature = "jdbc_mssql",
    feature = "jdbc_oracle",
    feature = "jdbc_db2"
))]
pub fn read_jdbc_to_polars(options: &JdbcOptions) -> Result<PlDataFrame, EngineError> {
    let runtime = crate::udf_context::get_thread_runtime_config();
    robin_sparkless_jdbc_polars::read_jdbc_to_polars(options, &runtime)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enforce_sql_trust_boundary_blocks_query_when_disabled() {
        unsafe {
            std::env::set_var("SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL", "false");
        }
        let mut m = HashMap::new();
        m.insert(
            "url".to_string(),
            "jdbc:postgresql://localhost/db".to_string(),
        );
        m.insert("query".to_string(), "SELECT 1".to_string());
        let opts = JdbcOptions::from_options_map(&m).unwrap();
        let err = opts.enforce_sql_trust_boundary().unwrap_err();
        assert!(
            err.to_string()
                .contains("query/sessionInitStatement/prepareQuery disabled")
        );
        unsafe {
            std::env::remove_var("SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL");
        }
    }

    #[test]
    fn jdbc_options_from_map_requires_url() {
        let mut m = HashMap::new();
        m.insert("dbtable".to_string(), "t".to_string());
        let r = JdbcOptions::from_options_map(&m);
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().to_lowercase().contains("url"));
    }

    #[test]
    fn jdbc_options_from_map_empty_url_key_not_present() {
        let m = HashMap::new();
        let r = JdbcOptions::from_options_map(&m);
        assert!(r.is_err());
    }

    #[test]
    fn jdbc_options_from_map_parses_options() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "postgres://localhost/db".to_string());
        m.insert("dbtable".to_string(), "mytable".to_string());
        m.insert("user".to_string(), "u".to_string());
        m.insert("fetchsize".to_string(), "1000".to_string());
        let r = JdbcOptions::from_options_map(&m).unwrap();
        assert_eq!(r.url, "postgres://localhost/db");
        assert_eq!(r.dbtable.as_deref(), Some("mytable"));
        assert_eq!(r.user.as_deref(), Some("u"));
        assert_eq!(r.fetch_size, Some(1000));
    }

    #[test]
    fn jdbc_options_from_map_parses_pyspark_camel_case_keys() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "jdbc:postgresql://host/db".to_string());
        m.insert("dbtable".to_string(), "t".to_string());
        m.insert("partitionColumn".to_string(), "id".to_string());
        m.insert("lowerBound".to_string(), "0".to_string());
        m.insert("upperBound".to_string(), "10000".to_string());
        m.insert("numPartitions".to_string(), "4".to_string());
        m.insert("batchsize".to_string(), "500".to_string());
        let r = JdbcOptions::from_options_map(&m).unwrap();
        assert_eq!(r.partition_column.as_deref(), Some("id"));
        assert_eq!(r.lower_bound, Some(0));
        assert_eq!(r.upper_bound, Some(10000));
        assert_eq!(r.num_partitions, Some(4));
        assert_eq!(r.batch_size, Some(500));
    }

    #[test]
    fn jdbc_options_invalid_lower_bound_returns_error() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "postgres://localhost/db".to_string());
        m.insert("lowerBound".to_string(), "not_a_number".to_string());
        let r = JdbcOptions::from_options_map(&m);
        assert!(r.is_err());
        let err = r.unwrap_err().to_string();
        assert!(
            err.to_lowercase().contains("lowerbound"),
            "error should mention lowerBound: {}",
            err
        );
        assert!(err.contains("not_a_number"));
    }

    #[test]
    fn jdbc_options_invalid_num_partitions_returns_error() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "postgres://localhost/db".to_string());
        m.insert("numPartitions".to_string(), "abc".to_string());
        let r = JdbcOptions::from_options_map(&m);
        assert!(r.is_err());
        let err = r.unwrap_err().to_string();
        assert!(
            err.to_lowercase().contains("numpartitions"),
            "error should mention numPartitions: {}",
            err
        );
    }

    #[test]
    fn jdbc_options_query_and_helpers() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "postgres://localhost/db".to_string());
        m.insert("query".to_string(), "SELECT 1 AS x".to_string());
        let r = JdbcOptions::from_options_map(&m).unwrap();
        assert!(r.is_query_based_read());
        assert!(!r.has_dbtable());
        assert_eq!(r.query.as_deref(), Some("SELECT 1 AS x"));
    }

    #[test]
    fn jdbc_options_dbtable_has_dbtable() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "postgres://localhost/db".to_string());
        m.insert("dbtable".to_string(), "mytable".to_string());
        let r = JdbcOptions::from_options_map(&m).unwrap();
        assert!(!r.is_query_based_read());
        assert!(r.has_dbtable());
    }

    #[test]
    fn jdbc_options_raw_options_preserves_all_keys() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "postgres://x/y".to_string());
        m.insert("dbtable".to_string(), "t".to_string());
        m.insert("customKey".to_string(), "customVal".to_string());
        let r = JdbcOptions::from_options_map(&m).unwrap();
        assert_eq!(
            r.raw_options.get("url").map(String::as_str),
            Some("postgres://x/y")
        );
        assert_eq!(
            r.raw_options.get("customKey").map(String::as_str),
            Some("customVal")
        );
    }

    #[test]
    fn jdbc_options_from_url_dbtable_and_properties() {
        let mut props = HashMap::new();
        props.insert("password".to_string(), "secret".to_string());
        let r = JdbcOptions::from_url_dbtable_and_properties(
            "postgres://h/d".to_string(),
            "tbl".to_string(),
            &props,
        )
        .unwrap();
        assert_eq!(r.url, "postgres://h/d");
        assert_eq!(r.dbtable.as_deref(), Some("tbl"));
        assert_eq!(r.password.as_deref(), Some("secret"));
    }

    #[test]
    fn jdbc_options_sqlite_and_postgres_url_detection() {
        assert!(super::is_sqlite_url("jdbc:sqlite:/tmp/db.db"));
        assert!(super::is_sqlite_url("jdbc:sqlite:file:foo.db"));
        assert!(super::is_sqlite_url("sqlite:/path/to/db"));
        assert!(!super::is_sqlite_url("jdbc:postgresql://localhost/db"));
        assert!(super::is_postgres_url("jdbc:postgresql://host/db"));
        assert!(super::is_postgres_url("postgres://localhost/db"));
        assert!(super::is_postgres_url("postgresql://localhost/db"));
        assert!(!super::is_postgres_url("jdbc:sqlite:mem"));
    }

    #[test]
    fn jdbc_options_from_url_dbtable_empty_properties() {
        let props = HashMap::new();
        let r = JdbcOptions::from_url_dbtable_and_properties(
            "postgresql://localhost/mydb".to_string(),
            "public.schema_table".to_string(),
            &props,
        )
        .unwrap();
        assert_eq!(r.url, "postgresql://localhost/mydb");
        assert_eq!(r.dbtable.as_deref(), Some("public.schema_table"));
        assert!(r.user.is_none());
        assert!(r.password.is_none());
    }

    #[test]
    fn driver_hint_matching_is_case_insensitive() {
        let driver = Some("Com.MySql.Cj.Jdbc.Driver".to_string());
        assert!(super::driver_hint_is(&driver, "com.mysql.cj.jdbc.Driver"));
    }

    #[test]
    fn jdbc_options_parses_new_pyspark_options() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "postgres://localhost/db".to_string());
        m.insert("dbtable".to_string(), "t".to_string());
        m.insert(
            "sessionInitStatement".to_string(),
            "SET timezone='UTC'".to_string(),
        );
        m.insert("queryTimeout".to_string(), "30".to_string());
        m.insert(
            "prepareQuery".to_string(),
            "WITH cte AS (SELECT 1)".to_string(),
        );
        m.insert(
            "customSchema".to_string(),
            "id DECIMAL(38,0), name STRING".to_string(),
        );
        m.insert("truncate".to_string(), "true".to_string());
        m.insert("cascadeTruncate".to_string(), "false".to_string());
        m.insert("isolationLevel".to_string(), "READ_COMMITTED".to_string());
        m.insert(
            "createTableOptions".to_string(),
            "ENGINE=InnoDB".to_string(),
        );
        m.insert(
            "createTableColumnTypes".to_string(),
            "name CHAR(64)".to_string(),
        );

        let opts = JdbcOptions::from_options_map(&m).unwrap();
        assert_eq!(
            opts.session_init_statement.as_deref(),
            Some("SET timezone='UTC'")
        );
        assert_eq!(opts.query_timeout, Some(30));
        assert_eq!(
            opts.prepare_query.as_deref(),
            Some("WITH cte AS (SELECT 1)")
        );
        assert_eq!(
            opts.custom_schema.as_deref(),
            Some("id DECIMAL(38,0), name STRING")
        );
        assert_eq!(opts.truncate, Some(true));
        assert_eq!(opts.cascade_truncate, Some(false));
        assert_eq!(opts.isolation_level.as_deref(), Some("READ_COMMITTED"));
        assert_eq!(opts.create_table_options.as_deref(), Some("ENGINE=InnoDB"));
        assert_eq!(
            opts.create_table_column_types.as_deref(),
            Some("name CHAR(64)")
        );
    }

    #[test]
    fn jdbc_options_truncate_parses_case_insensitive() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "postgres://localhost/db".to_string());
        m.insert("truncate".to_string(), "TRUE".to_string());
        let opts = JdbcOptions::from_options_map(&m).unwrap();
        assert_eq!(opts.truncate, Some(true));

        let mut m2 = HashMap::new();
        m2.insert("url".to_string(), "postgres://localhost/db".to_string());
        m2.insert("truncate".to_string(), "False".to_string());
        let opts2 = JdbcOptions::from_options_map(&m2).unwrap();
        assert_eq!(opts2.truncate, Some(false));
    }

    #[test]
    fn jdbc_options_new_fields_default_to_none() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "postgres://localhost/db".to_string());
        let opts = JdbcOptions::from_options_map(&m).unwrap();
        assert!(opts.session_init_statement.is_none());
        assert!(opts.query_timeout.is_none());
        assert!(opts.prepare_query.is_none());
        assert!(opts.custom_schema.is_none());
        assert!(opts.truncate.is_none());
        assert!(opts.cascade_truncate.is_none());
        assert!(opts.isolation_level.is_none());
        assert!(opts.create_table_options.is_none());
        assert!(opts.create_table_column_types.is_none());
    }

    #[test]
    fn routing_detects_jdbc_schemes() {
        assert!(is_mysql_url("jdbc:mysql://localhost:3306/db"));
        assert!(is_mariadb_url("jdbc:mariadb://localhost:3306/db"));
        assert!(is_mssql_url(
            "jdbc:sqlserver://localhost:1433;databaseName=db"
        ));
        assert!(is_oracle_url("jdbc:oracle:thin:@//localhost:1521/XEPDB1"));
        assert!(is_db2_url("jdbc:db2://localhost:50000/db"));
    }

    #[test]
    #[cfg(not(all(
        feature = "jdbc_mysql",
        feature = "jdbc_mariadb",
        feature = "jdbc_mssql",
        feature = "jdbc_oracle",
        feature = "jdbc_db2"
    )))]
    fn routing_errors_for_missing_backend_features() {
        // These should fail with a clear message when the specific backend feature
        // is not enabled (we run this test suite with only jdbc/sqlite enabled).
        // This test is skipped when all backend features are enabled.
        #[cfg(not(feature = "jdbc_mysql"))]
        {
            let opts = JdbcOptions {
                url: "jdbc:mysql://localhost/db".to_string(),
                dbtable: Some("t".to_string()),
                query: None,
                user: None,
                password: None,
                driver: None,
                partition_column: None,
                lower_bound: None,
                upper_bound: None,
                num_partitions: None,
                fetch_size: None,
                batch_size: None,
                session_init_statement: None,
                query_timeout: None,
                prepare_query: None,
                custom_schema: None,
                truncate: None,
                cascade_truncate: None,
                isolation_level: None,
                create_table_options: None,
                create_table_column_types: None,
                raw_options: HashMap::new(),
            };
            let err = read_jdbc_to_polars(&opts).unwrap_err().to_string();
            assert!(
                err.contains("jdbc_mysql"),
                "expected error to mention jdbc_mysql, got: {err}"
            );
        }
        #[cfg(not(feature = "jdbc_mariadb"))]
        {
            let opts = JdbcOptions {
                url: "jdbc:mariadb://localhost/db".to_string(),
                dbtable: Some("t".to_string()),
                query: None,
                user: None,
                password: None,
                driver: None,
                partition_column: None,
                lower_bound: None,
                upper_bound: None,
                num_partitions: None,
                fetch_size: None,
                batch_size: None,
                session_init_statement: None,
                query_timeout: None,
                prepare_query: None,
                custom_schema: None,
                truncate: None,
                cascade_truncate: None,
                isolation_level: None,
                create_table_options: None,
                create_table_column_types: None,
                raw_options: HashMap::new(),
            };
            let err = read_jdbc_to_polars(&opts).unwrap_err().to_string();
            assert!(
                err.contains("jdbc_mariadb"),
                "expected error to mention jdbc_mariadb, got: {err}"
            );
        }
        #[cfg(not(feature = "jdbc_mssql"))]
        {
            let opts = JdbcOptions {
                url: "jdbc:sqlserver://localhost;databaseName=db".to_string(),
                dbtable: Some("t".to_string()),
                query: None,
                user: None,
                password: None,
                driver: None,
                partition_column: None,
                lower_bound: None,
                upper_bound: None,
                num_partitions: None,
                fetch_size: None,
                batch_size: None,
                session_init_statement: None,
                query_timeout: None,
                prepare_query: None,
                custom_schema: None,
                truncate: None,
                cascade_truncate: None,
                isolation_level: None,
                create_table_options: None,
                create_table_column_types: None,
                raw_options: HashMap::new(),
            };
            let err = read_jdbc_to_polars(&opts).unwrap_err().to_string();
            assert!(
                err.contains("jdbc_mssql"),
                "expected error to mention jdbc_mssql, got: {err}"
            );
        }
        #[cfg(not(feature = "jdbc_oracle"))]
        {
            let opts = JdbcOptions {
                url: "jdbc:oracle:thin:@//localhost:1521/XEPDB1".to_string(),
                dbtable: Some("t".to_string()),
                query: None,
                user: None,
                password: None,
                driver: None,
                partition_column: None,
                lower_bound: None,
                upper_bound: None,
                num_partitions: None,
                fetch_size: None,
                batch_size: None,
                session_init_statement: None,
                query_timeout: None,
                prepare_query: None,
                custom_schema: None,
                truncate: None,
                cascade_truncate: None,
                isolation_level: None,
                create_table_options: None,
                create_table_column_types: None,
                raw_options: HashMap::new(),
            };
            let err = read_jdbc_to_polars(&opts).unwrap_err().to_string();
            assert!(
                err.contains("jdbc_oracle"),
                "expected error to mention jdbc_oracle, got: {err}"
            );
        }
        #[cfg(not(feature = "jdbc_db2"))]
        {
            let opts = JdbcOptions {
                url: "jdbc:db2://localhost:50000/db".to_string(),
                dbtable: Some("t".to_string()),
                query: None,
                user: None,
                password: None,
                driver: None,
                partition_column: None,
                lower_bound: None,
                upper_bound: None,
                num_partitions: None,
                fetch_size: None,
                batch_size: None,
                session_init_statement: None,
                query_timeout: None,
                prepare_query: None,
                custom_schema: None,
                truncate: None,
                cascade_truncate: None,
                isolation_level: None,
                create_table_options: None,
                create_table_column_types: None,
                raw_options: HashMap::new(),
            };
            let err = read_jdbc_to_polars(&opts).unwrap_err().to_string();
            assert!(
                err.contains("jdbc_db2"),
                "expected error to mention jdbc_db2, got: {err}"
            );
        }
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_read_write_roundtrip() {
        use crate::dataframe::SaveMode;
        use polars::prelude::NamedFrom;
        use polars::prelude::Series;

        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)", [])
            .unwrap();
        drop(conn);

        let id_series = Series::new("id".into(), vec![1i64, 2i64]);
        let name_series = Series::new("name".into(), vec!["a", "b"]);
        let df = PlDataFrame::new_infer_height(vec![id_series.into(), name_series.into()]).unwrap();

        let opts = JdbcOptions {
            url: url.clone(),
            dbtable: Some("t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            session_init_statement: None,
            query_timeout: None,
            prepare_query: None,
            custom_schema: None,
            truncate: None,
            cascade_truncate: None,
            isolation_level: None,
            create_table_options: None,
            create_table_column_types: None,
            raw_options: HashMap::new(),
        };
        super::write_jdbc_from_polars(&df, &opts, SaveMode::Append).unwrap();

        let read_df = super::read_jdbc_to_polars(&opts).unwrap();
        assert_eq!(read_df.height(), 2);
        let names: Vec<&str> = read_df
            .get_column_names()
            .iter()
            .map(|n| n.as_str())
            .collect();
        assert_eq!(names, ["id", "name"]);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_overwrite_replaces_content() {
        use crate::dataframe::SaveMode;
        use polars::prelude::{NamedFrom, Series};

        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)", [])
            .unwrap();
        drop(conn);

        let opts = JdbcOptions {
            url: url.clone(),
            dbtable: Some("t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            session_init_statement: None,
            query_timeout: None,
            prepare_query: None,
            custom_schema: None,
            truncate: None,
            cascade_truncate: None,
            isolation_level: None,
            create_table_options: None,
            create_table_column_types: None,
            raw_options: HashMap::new(),
        };

        let df1 = PlDataFrame::new_infer_height(vec![
            Series::new("id".into(), vec![1i64, 2i64]).into(),
            Series::new("name".into(), vec!["a", "b"]).into(),
        ])
        .unwrap();
        super::write_jdbc_from_polars(&df1, &opts, SaveMode::Append).unwrap();

        let df2 = PlDataFrame::new_infer_height(vec![
            Series::new("id".into(), vec![10i64]).into(),
            Series::new("name".into(), vec!["overwritten"]).into(),
        ])
        .unwrap();
        super::write_jdbc_from_polars(&df2, &opts, SaveMode::Overwrite).unwrap();

        let read_df = super::read_jdbc_to_polars(&opts).unwrap();
        assert_eq!(read_df.height(), 1);
        let names: Vec<&str> = read_df
            .get_column_names()
            .iter()
            .map(|n| n.as_str())
            .collect();
        assert_eq!(names, ["id", "name"]);
        let id_val: i64 = read_df
            .column("id")
            .unwrap()
            .get(0)
            .unwrap()
            .try_extract::<i64>()
            .unwrap();
        assert_eq!(id_val, 10);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_read_with_query_option() {
        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)", [])
            .unwrap();
        conn.execute(
            "INSERT INTO t (id, name) VALUES (1, 'one'), (2, 'two'), (3, 'three')",
            [],
        )
        .unwrap();
        drop(conn);

        let opts = JdbcOptions {
            url,
            dbtable: None,
            query: Some("SELECT id, name FROM t WHERE id = 2".to_string()),
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            session_init_statement: None,
            query_timeout: None,
            prepare_query: None,
            custom_schema: None,
            truncate: None,
            cascade_truncate: None,
            isolation_level: None,
            create_table_options: None,
            create_table_column_types: None,
            raw_options: HashMap::new(),
        };
        let read_df = super::read_jdbc_to_polars(&opts).unwrap();
        assert_eq!(read_df.height(), 1);
        let names: Vec<&str> = read_df
            .get_column_names()
            .iter()
            .map(|n| n.as_str())
            .collect();
        assert_eq!(names, ["id", "name"]);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_read_empty_table() {
        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute("CREATE TABLE empty_t (a INTEGER, b TEXT)", [])
            .unwrap();
        drop(conn);

        let opts = JdbcOptions {
            url,
            dbtable: Some("empty_t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            session_init_statement: None,
            query_timeout: None,
            prepare_query: None,
            custom_schema: None,
            truncate: None,
            cascade_truncate: None,
            isolation_level: None,
            create_table_options: None,
            create_table_column_types: None,
            raw_options: HashMap::new(),
        };
        let read_df = super::read_jdbc_to_polars(&opts).unwrap();
        assert_eq!(read_df.height(), 0);
        if read_df.get_column_names().len() >= 2 {
            let names: Vec<&str> = read_df
                .get_column_names()
                .iter()
                .map(|n| n.as_str())
                .collect();
            assert_eq!(names, ["a", "b"]);
        }
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_multiple_types_and_nulls() {
        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute(
            "CREATE TABLE types_t (id INTEGER, score REAL, label TEXT)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO types_t (id, score, label) VALUES (1, 3.125, 'test'), (2, NULL, 'no_score'), (3, 0.5, NULL)",
            [],
        )
        .unwrap();
        drop(conn);

        let opts = JdbcOptions {
            url,
            dbtable: Some("types_t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            session_init_statement: None,
            query_timeout: None,
            prepare_query: None,
            custom_schema: None,
            truncate: None,
            cascade_truncate: None,
            isolation_level: None,
            create_table_options: None,
            create_table_column_types: None,
            raw_options: HashMap::new(),
        };
        let read_df = super::read_jdbc_to_polars(&opts).unwrap();
        assert_eq!(read_df.height(), 3);
        let names: Vec<&str> = read_df
            .get_column_names()
            .iter()
            .map(|n| n.as_str())
            .collect();
        assert_eq!(names, ["id", "score", "label"]);
        let id_col = read_df.column("id").unwrap();
        assert_eq!(id_col.get(0).unwrap().try_extract::<i64>().unwrap(), 1);
        assert_eq!(id_col.get(1).unwrap().try_extract::<i64>().unwrap(), 2);
        assert_eq!(id_col.get(2).unwrap().try_extract::<i64>().unwrap(), 3);
        let score_col = read_df.column("score").unwrap();
        assert!((score_col.get(0).unwrap().try_extract::<f64>().unwrap() - 3.125).abs() < 1e-6);
        assert!(score_col.get(1).unwrap().is_null());
        assert!((score_col.get(2).unwrap().try_extract::<f64>().unwrap() - 0.5).abs() < 1e-6);
        let label_col = read_df.column("label").unwrap();
        assert_eq!(label_col.get(0).unwrap().to_string(), "\"test\"");
        assert_eq!(label_col.get(1).unwrap().to_string(), "\"no_score\"");
        assert!(label_col.get(2).unwrap().is_null());
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_write_empty_dataframe_no_op() {
        use crate::dataframe::SaveMode;
        use polars::prelude::{NamedFrom, Series};

        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER)", []).unwrap();
        conn.execute("INSERT INTO t (id) VALUES (1)", []).unwrap();
        drop(conn);

        let opts = JdbcOptions {
            url: url.clone(),
            dbtable: Some("t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            session_init_statement: None,
            query_timeout: None,
            prepare_query: None,
            custom_schema: None,
            truncate: None,
            cascade_truncate: None,
            isolation_level: None,
            create_table_options: None,
            create_table_column_types: None,
            raw_options: HashMap::new(),
        };

        let empty_df =
            PlDataFrame::new_infer_height(vec![Series::new("id".into(), Vec::<i64>::new()).into()])
                .unwrap();
        super::write_jdbc_from_polars(&empty_df, &opts, SaveMode::Append).unwrap();

        let read_df = super::read_jdbc_to_polars(&opts).unwrap();
        assert_eq!(
            read_df.height(),
            1,
            "empty write should not remove existing row"
        );
    }

    #[cfg(all(feature = "sqlite", not(feature = "jdbc_mysql")))]
    #[test]
    fn sqlite_unsupported_url_returns_clear_error() {
        // This test checks that a MySQL URL returns an unsupported error
        // when the jdbc_mysql feature is not enabled.
        let opts = JdbcOptions {
            url: "jdbc:mysql://localhost/db".to_string(),
            dbtable: Some("t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            session_init_statement: None,
            query_timeout: None,
            prepare_query: None,
            custom_schema: None,
            truncate: None,
            cascade_truncate: None,
            isolation_level: None,
            create_table_options: None,
            create_table_column_types: None,
            raw_options: HashMap::new(),
        };
        let r = super::read_jdbc_to_polars(&opts);
        assert!(r.is_err());
        let err = r.unwrap_err().to_string();
        assert!(
            err.to_lowercase().contains("unsupported") || err.to_lowercase().contains("url"),
            "{}",
            err
        );
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_error_if_exists_with_empty_table() {
        use crate::dataframe::SaveMode;
        use polars::prelude::{NamedFrom, Series};

        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)", [])
            .unwrap();
        drop(conn);

        let df = PlDataFrame::new_infer_height(vec![
            Series::new("id".into(), vec![1i64]).into(),
            Series::new("name".into(), vec!["test"]).into(),
        ])
        .unwrap();

        let opts = JdbcOptions {
            url: url.clone(),
            dbtable: Some("t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            session_init_statement: None,
            query_timeout: None,
            prepare_query: None,
            custom_schema: None,
            truncate: None,
            cascade_truncate: None,
            isolation_level: None,
            create_table_options: None,
            create_table_column_types: None,
            raw_options: HashMap::new(),
        };

        // ErrorIfExists should succeed on empty table
        let result = super::write_jdbc_from_polars(&df, &opts, SaveMode::ErrorIfExists);
        assert!(
            result.is_ok(),
            "ErrorIfExists should succeed on empty table"
        );

        let read_df = super::read_jdbc_to_polars(&opts).unwrap();
        assert_eq!(read_df.height(), 1);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_error_if_exists_with_data_fails() {
        use crate::dataframe::SaveMode;
        use polars::prelude::{NamedFrom, Series};

        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO t (id, name) VALUES (1, 'existing')", [])
            .unwrap();
        drop(conn);

        let df = PlDataFrame::new_infer_height(vec![
            Series::new("id".into(), vec![2i64]).into(),
            Series::new("name".into(), vec!["new"]).into(),
        ])
        .unwrap();

        let opts = JdbcOptions {
            url: url.clone(),
            dbtable: Some("t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            session_init_statement: None,
            query_timeout: None,
            prepare_query: None,
            custom_schema: None,
            truncate: None,
            cascade_truncate: None,
            isolation_level: None,
            create_table_options: None,
            create_table_column_types: None,
            raw_options: HashMap::new(),
        };

        // ErrorIfExists should fail when table has data
        let result = super::write_jdbc_from_polars(&df, &opts, SaveMode::ErrorIfExists);
        assert!(
            result.is_err(),
            "ErrorIfExists should fail when table has data"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("ErrorIfExists") || err.contains("already has data"),
            "Error should mention ErrorIfExists: {err}"
        );
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_ignore_mode_does_nothing_with_existing_data() {
        use crate::dataframe::SaveMode;
        use polars::prelude::{NamedFrom, Series};

        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO t (id, name) VALUES (1, 'existing')", [])
            .unwrap();
        drop(conn);

        let df = PlDataFrame::new_infer_height(vec![
            Series::new("id".into(), vec![2i64]).into(),
            Series::new("name".into(), vec!["new"]).into(),
        ])
        .unwrap();

        let opts = JdbcOptions {
            url: url.clone(),
            dbtable: Some("t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            session_init_statement: None,
            query_timeout: None,
            prepare_query: None,
            custom_schema: None,
            truncate: None,
            cascade_truncate: None,
            isolation_level: None,
            create_table_options: None,
            create_table_column_types: None,
            raw_options: HashMap::new(),
        };

        // Ignore mode should succeed silently without changing data
        let result = super::write_jdbc_from_polars(&df, &opts, SaveMode::Ignore);
        assert!(result.is_ok(), "Ignore mode should succeed");

        let read_df = super::read_jdbc_to_polars(&opts).unwrap();
        assert_eq!(read_df.height(), 1, "Ignore mode should not add new rows");

        let id_val: i64 = read_df
            .column("id")
            .unwrap()
            .get(0)
            .unwrap()
            .try_extract::<i64>()
            .unwrap();
        assert_eq!(id_val, 1, "Original data should be unchanged");
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_ignore_mode_writes_to_empty_table() {
        use crate::dataframe::SaveMode;
        use polars::prelude::{NamedFrom, Series};

        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)", [])
            .unwrap();
        drop(conn);

        let df = PlDataFrame::new_infer_height(vec![
            Series::new("id".into(), vec![1i64]).into(),
            Series::new("name".into(), vec!["new"]).into(),
        ])
        .unwrap();

        let opts = JdbcOptions {
            url: url.clone(),
            dbtable: Some("t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            session_init_statement: None,
            query_timeout: None,
            prepare_query: None,
            custom_schema: None,
            truncate: None,
            cascade_truncate: None,
            isolation_level: None,
            create_table_options: None,
            create_table_column_types: None,
            raw_options: HashMap::new(),
        };

        // Ignore mode should write to empty table
        let result = super::write_jdbc_from_polars(&df, &opts, SaveMode::Ignore);
        assert!(result.is_ok(), "Ignore mode should succeed on empty table");

        let read_df = super::read_jdbc_to_polars(&opts).unwrap();
        assert_eq!(
            read_df.height(),
            1,
            "Ignore mode should insert into empty table"
        );
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_session_init_statement_executes() {
        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER)", []).unwrap();
        conn.execute("INSERT INTO t (id) VALUES (1), (2)", [])
            .unwrap();
        drop(conn);

        let opts = JdbcOptions {
            url: url.clone(),
            dbtable: Some("t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            session_init_statement: Some("PRAGMA busy_timeout = 5000".to_string()),
            query_timeout: None,
            prepare_query: None,
            custom_schema: None,
            truncate: None,
            cascade_truncate: None,
            isolation_level: None,
            create_table_options: None,
            create_table_column_types: None,
            raw_options: HashMap::new(),
        };

        // Should succeed with session init statement
        let result = super::read_jdbc_to_polars(&opts);
        assert!(
            result.is_ok(),
            "Read with sessionInitStatement should succeed"
        );
        assert_eq!(result.unwrap().height(), 2);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_prepare_query_creates_temp_table() {
        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute("CREATE TABLE source (id INTEGER, val TEXT)", [])
            .unwrap();
        conn.execute(
            "INSERT INTO source (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            [],
        )
        .unwrap();
        drop(conn);

        let opts = JdbcOptions {
            url: url.clone(),
            dbtable: None,
            query: Some("SELECT * FROM temp_view WHERE id > 1".to_string()),
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            session_init_statement: None,
            query_timeout: None,
            prepare_query: Some(
                "CREATE TEMPORARY VIEW temp_view AS SELECT * FROM source".to_string(),
            ),
            custom_schema: None,
            truncate: None,
            cascade_truncate: None,
            isolation_level: None,
            create_table_options: None,
            create_table_column_types: None,
            raw_options: HashMap::new(),
        };

        let result = super::read_jdbc_to_polars(&opts);
        assert!(
            result.is_ok(),
            "Read with prepareQuery should succeed: {:?}",
            result.err()
        );
        let df = result.unwrap();
        assert_eq!(df.height(), 2, "Should return rows where id > 1");
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_batchsize_handles_large_data() {
        use crate::dataframe::SaveMode;
        use polars::prelude::{NamedFrom, Series};

        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)", [])
            .unwrap();
        drop(conn);

        // Create a DataFrame with 2500 rows to test batch boundaries
        let ids: Vec<i64> = (0..2500).collect();
        let names: Vec<String> = (0..2500).map(|i| format!("name_{i}")).collect();
        let df = PlDataFrame::new_infer_height(vec![
            Series::new("id".into(), ids).into(),
            Series::new("name".into(), names).into(),
        ])
        .unwrap();

        let opts = JdbcOptions {
            url: url.clone(),
            dbtable: Some("t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: Some(500), // Use small batch size to test multiple batches
            session_init_statement: None,
            query_timeout: None,
            prepare_query: None,
            custom_schema: None,
            truncate: None,
            cascade_truncate: None,
            isolation_level: None,
            create_table_options: None,
            create_table_column_types: None,
            raw_options: HashMap::new(),
        };

        let result = super::write_jdbc_from_polars(&df, &opts, SaveMode::Append);
        assert!(
            result.is_ok(),
            "Write with batchsize should succeed: {:?}",
            result.err()
        );

        let read_df = super::read_jdbc_to_polars(&opts).unwrap();
        assert_eq!(read_df.height(), 2500, "All 2500 rows should be written");
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_append_mode_adds_to_existing() {
        use crate::dataframe::SaveMode;
        use polars::prelude::{NamedFrom, Series};

        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER)", []).unwrap();
        conn.execute("INSERT INTO t (id) VALUES (1), (2)", [])
            .unwrap();
        drop(conn);

        let df =
            PlDataFrame::new_infer_height(vec![Series::new("id".into(), vec![3i64, 4i64]).into()])
                .unwrap();

        let opts = JdbcOptions {
            url: url.clone(),
            dbtable: Some("t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            session_init_statement: None,
            query_timeout: None,
            prepare_query: None,
            custom_schema: None,
            truncate: None,
            cascade_truncate: None,
            isolation_level: None,
            create_table_options: None,
            create_table_column_types: None,
            raw_options: HashMap::new(),
        };

        super::write_jdbc_from_polars(&df, &opts, SaveMode::Append).unwrap();

        let read_df = super::read_jdbc_to_polars(&opts).unwrap();
        assert_eq!(read_df.height(), 4, "Append should add to existing data");
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_boolean_values_roundtrip() {
        use crate::dataframe::SaveMode;
        use polars::prelude::{NamedFrom, Series};

        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, flag INTEGER)", [])
            .unwrap();
        drop(conn);

        let df = PlDataFrame::new_infer_height(vec![
            Series::new("id".into(), vec![1i64, 2i64]).into(),
            Series::new("flag".into(), vec![true, false]).into(),
        ])
        .unwrap();

        let opts = JdbcOptions {
            url: url.clone(),
            dbtable: Some("t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            session_init_statement: None,
            query_timeout: None,
            prepare_query: None,
            custom_schema: None,
            truncate: None,
            cascade_truncate: None,
            isolation_level: None,
            create_table_options: None,
            create_table_column_types: None,
            raw_options: HashMap::new(),
        };

        super::write_jdbc_from_polars(&df, &opts, SaveMode::Append).unwrap();

        let read_df = super::read_jdbc_to_polars(&opts).unwrap();
        assert_eq!(read_df.height(), 2);

        let flag_col = read_df.column("flag").unwrap();
        let val1: i64 = flag_col.get(0).unwrap().try_extract().unwrap();
        let val2: i64 = flag_col.get(1).unwrap().try_extract().unwrap();
        assert_eq!(val1, 1, "true should be stored as 1");
        assert_eq!(val2, 0, "false should be stored as 0");
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_float_precision_maintained() {
        use crate::dataframe::SaveMode;
        use polars::prelude::{NamedFrom, Series};

        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute("CREATE TABLE t (val REAL)", []).unwrap();
        drop(conn);

        let df = PlDataFrame::new_infer_height(vec![
            Series::new("val".into(), vec![1.23456789012f64, -273.15f64, 1e-10f64]).into(),
        ])
        .unwrap();

        let opts = JdbcOptions {
            url: url.clone(),
            dbtable: Some("t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            session_init_statement: None,
            query_timeout: None,
            prepare_query: None,
            custom_schema: None,
            truncate: None,
            cascade_truncate: None,
            isolation_level: None,
            create_table_options: None,
            create_table_column_types: None,
            raw_options: HashMap::new(),
        };

        super::write_jdbc_from_polars(&df, &opts, SaveMode::Append).unwrap();

        let read_df = super::read_jdbc_to_polars(&opts).unwrap();
        let val_col = read_df.column("val").unwrap();

        let v1: f64 = val_col.get(0).unwrap().try_extract().unwrap();
        let v2: f64 = val_col.get(1).unwrap().try_extract().unwrap();
        let v3: f64 = val_col.get(2).unwrap().try_extract().unwrap();

        assert!(
            (v1 - 1.23456789012).abs() < 1e-10,
            "Float precision should be preserved: {v1}"
        );
        assert!(
            (v2 - (-273.15)).abs() < 1e-10,
            "Negative float should be preserved: {v2}"
        );
        assert!(
            (v3 - 1e-10).abs() < 1e-15,
            "Small float should be preserved: {v3}"
        );
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_special_characters_in_strings() {
        use crate::dataframe::SaveMode;
        use polars::prelude::{NamedFrom, Series};

        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.execute("CREATE TABLE t (text TEXT)", []).unwrap();
        drop(conn);

        let df = PlDataFrame::new_infer_height(vec![
            Series::new(
                "text".into(),
                vec![
                    "Hello 'World'",
                    "Line1\nLine2",
                    "Tab\there",
                    "Unicode: 日本語 émoji 🎉",
                    "",
                ],
            )
            .into(),
        ])
        .unwrap();

        let opts = JdbcOptions {
            url: url.clone(),
            dbtable: Some("t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            session_init_statement: None,
            query_timeout: None,
            prepare_query: None,
            custom_schema: None,
            truncate: None,
            cascade_truncate: None,
            isolation_level: None,
            create_table_options: None,
            create_table_column_types: None,
            raw_options: HashMap::new(),
        };

        super::write_jdbc_from_polars(&df, &opts, SaveMode::Append).unwrap();

        let read_df = super::read_jdbc_to_polars(&opts).unwrap();
        assert_eq!(read_df.height(), 5);

        let text_col = read_df.column("text").unwrap();

        // Check quotes are preserved
        let v0 = text_col.get(0).unwrap().to_string();
        assert!(v0.contains("'World'"), "Single quotes should be preserved");

        // Check unicode is preserved
        let v3 = text_col.get(3).unwrap().to_string();
        assert!(v3.contains("日本語"), "Unicode should be preserved");
    }

    #[test]
    fn jdbc_options_query_timeout_validation() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "postgres://localhost/db".to_string());
        m.insert("queryTimeout".to_string(), "not_a_number".to_string());

        let result = JdbcOptions::from_options_map(&m);
        assert!(result.is_err(), "Invalid queryTimeout should fail");
        let err = result.unwrap_err().to_string();
        assert!(
            err.to_lowercase().contains("querytimeout"),
            "Error should mention queryTimeout: {err}"
        );
    }

    #[test]
    fn jdbc_options_batch_size_validation() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "postgres://localhost/db".to_string());
        m.insert("batchsize".to_string(), "abc".to_string());

        let result = JdbcOptions::from_options_map(&m);
        assert!(result.is_err(), "Invalid batchsize should fail");
    }

    #[test]
    fn jdbc_options_partition_bounds_validation() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "postgres://localhost/db".to_string());
        m.insert("lowerBound".to_string(), "0".to_string());
        m.insert("upperBound".to_string(), "not_a_number".to_string());

        let result = JdbcOptions::from_options_map(&m);
        assert!(result.is_err(), "Invalid upperBound should fail");
    }

    #[test]
    fn jdbc_options_combined_read_write_options() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "postgres://localhost/db".to_string());
        m.insert("dbtable".to_string(), "users".to_string());
        m.insert("user".to_string(), "admin".to_string());
        m.insert("password".to_string(), "secret".to_string());
        m.insert(
            "sessionInitStatement".to_string(),
            "SET search_path TO myschema".to_string(),
        );
        m.insert("queryTimeout".to_string(), "60".to_string());
        m.insert("fetchsize".to_string(), "1000".to_string());
        m.insert("batchsize".to_string(), "500".to_string());
        m.insert("truncate".to_string(), "false".to_string());
        m.insert("partitionColumn".to_string(), "id".to_string());
        m.insert("lowerBound".to_string(), "0".to_string());
        m.insert("upperBound".to_string(), "1000000".to_string());
        m.insert("numPartitions".to_string(), "10".to_string());

        let opts = JdbcOptions::from_options_map(&m).unwrap();

        assert_eq!(opts.url, "postgres://localhost/db");
        assert_eq!(opts.dbtable.as_deref(), Some("users"));
        assert_eq!(opts.user.as_deref(), Some("admin"));
        assert_eq!(opts.password.as_deref(), Some("secret"));
        assert_eq!(
            opts.session_init_statement.as_deref(),
            Some("SET search_path TO myschema")
        );
        assert_eq!(opts.query_timeout, Some(60));
        assert_eq!(opts.fetch_size, Some(1000));
        assert_eq!(opts.batch_size, Some(500));
        assert_eq!(opts.truncate, Some(false));
        assert_eq!(opts.partition_column.as_deref(), Some("id"));
        assert_eq!(opts.lower_bound, Some(0));
        assert_eq!(opts.upper_bound, Some(1000000));
        assert_eq!(opts.num_partitions, Some(10));
    }
}
