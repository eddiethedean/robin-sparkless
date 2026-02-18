//! Regression tests for issue #543 – CSV inferSchema behavior (PySpark parity).
//!
//! With inferSchema=True, CSV columns should be inferred as int/long, double, boolean, etc.

mod common;

use common::spark;
use polars::prelude::DataType;
use std::io::Write;
use tempfile::NamedTempFile;

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
