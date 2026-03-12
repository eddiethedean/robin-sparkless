//! JDBC read/write example for PostgreSQL.
//!
//! Build and run with the `jdbc` feature and a connection URL:
//!
//!   cargo run --example jdbc_postgres --features jdbc
//!
//! Set environment variable ROBIN_SPARKLESS_JDBC_URL (e.g. postgres://user:password@localhost:5432/mydb).
//! Optionally set ROBIN_SPARKLESS_JDBC_TABLE (default: sparkless_jdbc_example).
//!
//! Prerequisites: a Postgres database and a table, e.g.:
//!
//!   CREATE TABLE sparkless_jdbc_example (id BIGINT PRIMARY KEY, name TEXT);

use std::collections::HashMap;

use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = match std::env::var("ROBIN_SPARKLESS_JDBC_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!(
                "Usage: ROBIN_SPARKLESS_JDBC_URL='postgres://user:password@localhost:5432/db' \
                 cargo run --example jdbc_postgres --features jdbc"
            );
            eprintln!("Optional: ROBIN_SPARKLESS_JDBC_TABLE (default: sparkless_jdbc_example)");
            std::process::exit(1);
        }
    };

    let table = std::env::var("ROBIN_SPARKLESS_JDBC_TABLE")
        .unwrap_or_else(|_| "sparkless_jdbc_example".to_string());

    let spark = robin_sparkless::SparkSession::builder()
        .app_name("jdbc_postgres_example")
        .get_or_create();

    let mut props = HashMap::new();
    if let Ok(u) = std::env::var("ROBIN_SPARKLESS_JDBC_USER") {
        props.insert("user".to_string(), u);
    }
    if let Ok(p) = std::env::var("ROBIN_SPARKLESS_JDBC_PASSWORD") {
        props.insert("password".to_string(), p);
    }

    // Read via spark.read.jdbc(url, table, properties)
    println!("Reading from JDBC table {}...", table);
    let df = spark.read().jdbc(&url, &table, &props)?;
    let n = df.count()?;
    println!("Row count: {}", n);
    df.show(Some(5))?;

    // Alternatively: format("jdbc").options(...).load(dummy_path)
    let _df2 = spark
        .read()
        .format("jdbc")
        .option("url", &url)
        .option("dbtable", &table)
        .options(props.clone())
        .load(".")?;

    // Write a small DataFrame back (append mode)
    let write_table = format!("{}_written", table);
    let tiny = spark.create_dataframe_from_rows(
        vec![
            vec![json!(100_i64), json!("from_rust")],
            vec![json!(101_i64), json!("jdbc_example")],
        ],
        vec![("id".to_string(), "bigint".to_string()), ("name".to_string(), "string".to_string())],
        false,
        false,
    )?;
    let props_vec: Vec<(String, String)> = props.into_iter().collect();
    println!("Writing 2 rows to {} (append)...", write_table);
    tiny.write().jdbc(
        &url,
        &write_table,
        &props_vec,
        robin_sparkless::SaveMode::Append,
    )?;
    println!("Done.");

    Ok(())
}
