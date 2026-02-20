//! CSV, create_dataframe from rows/JSON, Delta read/write.
//!
//! Merged from: issue_543, issue_633_634_647, delta_core.

mod common;

use common::spark;
use polars::prelude::DataType;
use robin_sparkless::DataFrame;
use robin_sparkless::plan;
use robin_sparkless::schema::StructType;
use robin_sparkless::schema::schema_from_json;
use serde_json::json;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use tempfile::NamedTempFile;

// ---------- issue_543 ----------

#[test]
fn issue_543_csv_infer_schema_types() {
    let spark = spark();
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "name,age,salary,active").unwrap();
    writeln!(f, "Alice,25,50000.5,true").unwrap();
    writeln!(f, "Bob,30,60000,false").unwrap();
    f.flush().unwrap();

    let df = spark.read_csv(f.path()).unwrap();

    let age_dtype = df.get_column_dtype("age").expect("age column");
    assert!(
        age_dtype.is_integer(),
        "age should be inferred as integer (PySpark long); got {:?}",
        age_dtype
    );
    let salary_dtype = df.get_column_dtype("salary").expect("salary column");
    assert!(
        salary_dtype.is_float() || salary_dtype.is_numeric(),
        "salary should be inferred as double; got {:?}",
        salary_dtype
    );
    let active_dtype = df.get_column_dtype("active").expect("active column");
    assert!(
        active_dtype == DataType::Boolean,
        "active should be inferred as boolean; got {:?}",
        active_dtype
    );
}

#[test]
fn issue_543_csv_reader_option_infer_schema() {
    let spark = spark();
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "name,age").unwrap();
    writeln!(f, "Alice,25").unwrap();
    f.flush().unwrap();

    let df = spark
        .read()
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f.path())
        .unwrap();
    let age_dtype = df.get_column_dtype("age").expect("age column");
    assert!(
        age_dtype.is_integer(),
        "read().option(inferSchema, true).csv() should infer age as integer; got {:?}",
        age_dtype
    );
}

// ---------- issue_633_634_647 ----------

#[test]
fn create_dataframe_from_rows_empty_schema_infers_from_json_rows() {
    let spark = spark();
    let schema: Vec<(String, String)> = vec![];
    let rows: Vec<Vec<serde_json::Value>> = vec![
        vec![json!("hello"), json!(42), json!(true)],
        vec![json!("world"), json!(0), json!(false)],
    ];
    let df = spark
        .create_dataframe_from_rows_engine(rows, schema)
        .expect("empty schema + non-empty rows should infer schema");
    assert_eq!(df.count_engine().unwrap(), 2);
    let names = df.columns_engine().unwrap();
    assert_eq!(names, vec!["c0", "c1", "c2"]);
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].get("c0").and_then(|v| v.as_str()), Some("hello"));
    assert_eq!(out[0].get("c1").and_then(|v| v.as_i64()), Some(42));
    assert_eq!(out[0].get("c2").and_then(|v| v.as_bool()), Some(true));
}

#[test]
fn create_dataframe_from_rows_collect_as_json_roundtrip() {
    let spark = spark();
    let schema = vec![
        ("id".to_string(), "bigint".to_string()),
        ("label".to_string(), "string".to_string()),
    ];
    let rows: Vec<Vec<serde_json::Value>> =
        vec![vec![json!(1), json!("a")], vec![json!(2), json!("b")]];
    let df = spark
        .create_dataframe_from_rows_engine(rows.clone(), schema)
        .unwrap();
    let collected = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(collected.len(), 2);
    assert_eq!(collected[0].get("id").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(
        collected[0].get("label").and_then(|v| v.as_str()),
        Some("a")
    );
    assert_eq!(collected[1].get("id").and_then(|v| v.as_i64()), Some(2));
    assert_eq!(
        collected[1].get("label").and_then(|v| v.as_str()),
        Some("b")
    );
}

#[test]
fn execute_plan_with_json_rows_data() {
    let spark = spark();
    let schema = vec![
        ("x".to_string(), "bigint".to_string()),
        ("y".to_string(), "string".to_string()),
    ];
    let rows: Vec<Vec<serde_json::Value>> =
        vec![vec![json!(10), json!("p")], vec![json!(20), json!("q")]];
    let plan = vec![
        json!({"op": "filter", "payload": {"op": "gt", "left": {"col": "x"}, "right": {"lit": 15}}}),
        json!({"op": "select", "payload": ["y", "x"]}),
    ];
    let df = plan::execute_plan(&spark, rows, schema, &plan).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].get("y").and_then(|v| v.as_str()), Some("q"));
    assert_eq!(out[0].get("x").and_then(|v| v.as_i64()), Some(20));
}

#[test]
fn schema_from_json_and_create_dataframe_from_rows() {
    let schema_json = r#"{"fields":[{"name":"id","data_type":"Long","nullable":true},{"name":"name","data_type":"String","nullable":true}]}"#;
    let _struct_type: StructType =
        schema_from_json(schema_json).expect("schema_from_json should parse");
    let spark = spark();
    let schema = vec![
        ("id".to_string(), "bigint".to_string()),
        ("name".to_string(), "string".to_string()),
    ];
    let rows: Vec<Vec<serde_json::Value>> =
        vec![vec![json!(1), json!("alice")], vec![json!(2), json!("bob")]];
    let df = spark
        .create_dataframe_from_rows_engine(rows, schema)
        .unwrap();
    assert_eq!(df.count_engine().unwrap(), 2);
}

// ---------- delta_core ----------

#[test]
fn write_and_read_delta_round_trip_core() {
    let spark = spark();
    let tmp_dir = tempfile::tempdir().unwrap();
    let path: PathBuf = tmp_dir.path().join("delta_table");

    let pl = polars::prelude::df![
        "id" => &[1i64, 2i64, 3i64],
        "v" => &[10i64, 20i64, 30i64],
        "name" => &["a", "b", "c"],
    ]
    .unwrap();
    let df: DataFrame = spark.create_dataframe_from_polars(pl);

    let write_result = df.write_delta(&path, true);
    if let Err(e) = write_result {
        let msg = format!("{e}");
        if msg.contains("requires the 'delta' feature") {
            return;
        }
        panic!("unexpected error from write_delta: {msg}");
    }

    assert!(
        fs::metadata(&path).is_ok(),
        "delta path should exist after write"
    );

    let back = spark.read_delta_from_path(&path);
    let back = match back {
        Ok(df) => df,
        Err(e) => {
            let msg = format!("{e}");
            if msg.contains("requires the 'delta' feature") {
                return;
            }
            panic!("unexpected error from read_delta_from_path: {msg}");
        }
    };

    let rows = back.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["id"].as_i64().unwrap(), 1);
    assert_eq!(rows[1]["id"].as_i64().unwrap(), 2);
    assert_eq!(rows[2]["id"].as_i64().unwrap(), 3);
}

#[test]
fn write_delta_append_and_overwrite_core() {
    let spark = spark();
    let tmp_dir = tempfile::tempdir().unwrap();
    let path: PathBuf = tmp_dir.path().join("delta_table_append");

    let pl1 = polars::prelude::df![
        "id" => &[1i64, 2i64],
        "v" => &[10i64, 20i64],
    ]
    .unwrap();
    let pl2 = polars::prelude::df![
        "id" => &[3i64],
        "v" => &[30i64],
    ]
    .unwrap();

    let df1: DataFrame = spark.create_dataframe_from_polars(pl1);
    let df2: DataFrame = spark.create_dataframe_from_polars(pl2);

    if let Err(e) = df1.write_delta(&path, true) {
        let msg = format!("{e}");
        if msg.contains("requires the 'delta' feature") {
            return;
        }
        panic!("unexpected error from first write_delta: {msg}");
    }

    if let Err(e) = df2.write_delta(&path, false) {
        let msg = format!("{e}");
        if msg.contains("requires the 'delta' feature") {
            return;
        }
        panic!("unexpected error from append write_delta: {msg}");
    }

    let back = match spark.read_delta_from_path(&path) {
        Ok(df) => df,
        Err(e) => {
            let msg = format!("{e}");
            if msg.contains("requires the 'delta' feature") {
                return;
            }
            panic!("unexpected error from read_delta_from_path: {msg}");
        }
    };

    let rows = back.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 3);
}
