use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::{NaiveDate, NaiveDateTime};
use polars::prelude::{
    col, len, lit, DataFrame as PlDataFrame, DataType, Expr, NamedFrom, PolarsError, Series,
    TimeUnit,
};
use robin_sparkless::plan;
use robin_sparkless::{DataFrame, JoinType, SparkSession};
use serde::Deserialize;
use serde_json::Value;

/// Top-level fixture structure, matching the JSON we’ll generate from PySpark.
#[derive(Debug, Deserialize)]
struct Fixture {
    name: String,
    #[allow(dead_code)]
    pyspark_version: Option<String>,
    input: InputSection,
    #[serde(default)]
    right_input: Option<InputSection>,
    operations: Vec<Operation>,
    expected: ExpectedSection,
    /// When true, skip this fixture (e.g. known unsupported op or semantic difference).
    #[serde(default)]
    skip: bool,
    #[serde(default)]
    #[allow(dead_code)]
    skip_reason: Option<String>,
    /// When true, expect execution to fail (e.g. assert_true on false, raise_error). Pass if Err.
    #[serde(default)]
    expect_error: bool,
    /// When true, substitute current_date/current_timestamp/curdate/now with fixed literals for deterministic tests.
    #[serde(default)]
    mock_dates: bool,
}

#[derive(Debug, Deserialize)]
struct InputSection {
    schema: Vec<ColumnSpec>,
    rows: Vec<Vec<Value>>,
    #[serde(default)]
    file_source: Option<FileSource>,
    #[serde(default)]
    table_source: Option<TableSource>,
}

#[derive(Debug, Deserialize)]
struct FileSource {
    format: String,  // "csv", "parquet", "json"
    content: String, // file content as string
    #[serde(default)]
    reader_options: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
struct TableSource {
    view_name: String,
}

#[derive(Debug, Deserialize)]
struct ColumnSpec {
    name: String,
    r#type: String,
}

#[derive(Debug, Deserialize)]
struct ExpectedSection {
    schema: Vec<ColumnSpec>,
    rows: Vec<Vec<Value>>,
}

/// Supported operations in the first parity slice.
#[derive(Debug, Deserialize)]
#[serde(tag = "op")]
enum Operation {
    #[serde(rename = "filter")]
    Filter { expr: String },
    #[serde(rename = "select")]
    Select { columns: Vec<String> },
    #[serde(rename = "orderBy")]
    OrderBy {
        columns: Vec<String>,
        #[serde(default)]
        ascending: Vec<bool>,
        #[serde(default)]
        nulls_first: Option<Vec<bool>>,
    },
    #[serde(rename = "groupBy")]
    GroupBy { columns: Vec<String> },
    #[serde(rename = "agg")]
    Agg { aggregations: Vec<AggregationSpec> },
    #[serde(rename = "withColumn")]
    WithColumn { column: String, expr: String },
    #[serde(rename = "join")]
    Join { on: Vec<String>, how: String },
    #[serde(rename = "window")]
    Window {
        column: String, // output column name
        func: String,
        partition_by: Vec<String>,
        #[serde(default)]
        order_by: Option<Vec<OrderBySpec>>,
        #[serde(default)]
        value_column: Option<String>, // for lag/lead/nth_value: column to use
        #[serde(default)]
        n: Option<i64>, // for ntile: number of buckets; for nth_value: 1-based index
    },
    #[serde(rename = "union")]
    Union {},
    #[serde(rename = "unionByName")]
    UnionByName {},
    #[serde(rename = "distinct")]
    Distinct {
        #[serde(default)]
        subset: Option<Vec<String>>,
    },
    #[serde(rename = "drop")]
    Drop { columns: Vec<String> },
    #[serde(rename = "dropna")]
    Dropna {
        #[serde(default)]
        subset: Option<Vec<String>>,
    },
    #[serde(rename = "fillna")]
    Fillna { value: serde_json::Value },
    #[serde(rename = "limit")]
    Limit { n: u64 },
    #[serde(rename = "withColumnRenamed")]
    WithColumnRenamed { existing: String, new: String },
    #[serde(rename = "replace")]
    Replace {
        column: String,
        old_value: String,
        new_value: String,
    },
    #[serde(rename = "crossJoin")]
    CrossJoin {},
    #[serde(rename = "describe")]
    Describe {},
    #[serde(rename = "summary")]
    Summary {},
    #[serde(rename = "subtract")]
    Subtract {},
    #[serde(rename = "intersect")]
    Intersect {},
    #[serde(rename = "first")]
    First {},
    #[serde(rename = "head")]
    Head { n: u64 },
    #[serde(rename = "isEmpty")]
    IsEmpty {},
    #[serde(rename = "offset")]
    Offset { n: u64 },
}

#[derive(Debug, Deserialize)]
struct OrderBySpec {
    col: String,
    #[serde(default)]
    asc: bool,
}

#[derive(Debug, Deserialize)]
struct AggregationSpec {
    func: String,
    #[allow(dead_code)]
    alias: String,
    #[serde(default)]
    column: Option<String>, // Column name for sum/avg/min/max (not needed for count)
    #[serde(default)]
    column2: Option<String>, // Second column for covar_pop, covar_samp, corr
    #[serde(default)]
    percentile: Option<f64>, // For percentile(column, p)
    #[serde(default)]
    value_column: Option<String>, // For max_by, min_by
    #[serde(default)]
    ord_column: Option<String>, // For max_by, min_by
}

/// Load phase manifest and return fixture names for the given phase.
/// Phase should be "a", "b", "c", etc.; looks up "phase_a", "phase_b", etc.
fn load_phase_fixture_names(phase: &str) -> Option<Vec<String>> {
    let manifest_path = Path::new("tests/fixtures/phase_manifest.json");
    if !manifest_path.exists() {
        return None;
    }
    let text = fs::read_to_string(manifest_path).ok()?;
    let manifest: HashMap<String, Vec<String>> = serde_json::from_str(&text).ok()?;
    let key = format!("phase_{}", phase.to_lowercase());
    manifest.get(&key).cloned()
}

/// Parity tests generated from PySpark fixtures.
///
/// This test reads JSON fixtures from `tests/fixtures/` and (if present)
/// `tests/fixtures/converted/` (from Sparkless expected_outputs via convert_sparkless_fixtures.py),
/// and verifies that robin-sparkless produces the same results as PySpark.
///
/// Set `PARITY_FIXTURE=<name>` (e.g. `PARITY_FIXTURE=groupby_count`) to run only the fixture
/// whose `name` matches, for faster iteration.
///
/// Set `PARITY_PHASE=<a|b|c|d|e|f|g>` to run only fixtures in that phase's manifest.
#[test]
fn pyspark_parity_fixtures() {
    run_pyspark_parity_fixtures(std::env::var("PARITY_PHASE").ok().as_deref());
}

/// Core parity runner. When `phase_filter` is Some("a") etc., only fixtures in that phase's
/// manifest are run. Phase E has no fixtures; the test passes with no fixtures run.
fn run_pyspark_parity_fixtures(phase_filter: Option<&str>) {
    let single = std::env::var("PARITY_FIXTURE").ok();

    let phase_names: Option<std::collections::HashSet<String>> = phase_filter
        .and_then(load_phase_fixture_names)
        .map(|v| v.into_iter().collect());

    let mut paths: Vec<std::path::PathBuf> = Vec::new();

    let fixtures_dir = Path::new("tests/fixtures");
    if fixtures_dir.exists() {
        for entry in fs::read_dir(fixtures_dir).expect("read fixtures directory") {
            let path = entry.expect("dir entry").path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            if path.file_name().and_then(|s| s.to_str()) == Some("phase_manifest.json") {
                continue;
            }
            if path.is_file() {
                paths.push(path);
            }
        }
    }

    let converted_dir = Path::new("tests/fixtures/converted");
    if converted_dir.exists() {
        for entry in fs::read_dir(converted_dir).expect("read fixtures/converted directory") {
            let path = entry.expect("dir entry").path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            if path.is_file() {
                paths.push(path);
            }
        }
    }

    let pyspark_extracted_dir = Path::new("tests/fixtures/pyspark_extracted");
    if pyspark_extracted_dir.exists() {
        for entry in
            fs::read_dir(pyspark_extracted_dir).expect("read fixtures/pyspark_extracted directory")
        {
            let path = entry.expect("dir entry").path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            if path.is_file() {
                paths.push(path);
            }
        }
    }

    let mut failures: Vec<(String, String)> = Vec::new();
    let mut ran_any = false;

    for path in paths {
        let text = fs::read_to_string(&path).expect("read fixture");
        let fixture: Fixture = serde_json::from_str(&text).expect("parse fixture json");

        if let Some(ref name_filter) = single {
            if &fixture.name != name_filter {
                continue;
            }
        }

        if let Some(ref names) = phase_names {
            if !names.contains(&fixture.name) {
                continue;
            }
        }

        if fixture.skip {
            continue;
        }

        ran_any = true;
        match run_fixture(&fixture) {
            Ok(()) => {}
            Err(e) => {
                failures.push((fixture.name.clone(), e.to_string()));
            }
        }
    }

    if let Some(ref name_filter) = single {
        assert!(
            ran_any,
            "PARITY_FIXTURE={name_filter} but no matching fixture found (check fixture name)"
        );
    }

    assert!(
        failures.is_empty(),
        "{} fixture(s) failed: {:?}",
        failures.len(),
        failures
            .iter()
            .map(|(name, err)| format!("{name}: {err}"))
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_parity_phase_a() {
    run_pyspark_parity_fixtures(Some("a"));
}

#[test]
fn test_parity_phase_b() {
    run_pyspark_parity_fixtures(Some("b"));
}

#[test]
fn test_parity_phase_c() {
    run_pyspark_parity_fixtures(Some("c"));
}

#[test]
fn test_parity_phase_d() {
    run_pyspark_parity_fixtures(Some("d"));
}

#[test]
fn test_parity_phase_e() {
    run_pyspark_parity_fixtures(Some("e"));
}

#[test]
fn test_parity_phase_f() {
    run_pyspark_parity_fixtures(Some("f"));
}

#[test]
fn test_parity_phase_g() {
    run_pyspark_parity_fixtures(Some("g"));
}

fn run_fixture(fixture: &Fixture) -> Result<(), PolarsError> {
    // Basic shape sanity.
    assert!(
        !fixture.input.schema.is_empty(),
        "fixture {} has empty schema",
        fixture.name
    );
    if !fixture.expect_error {
        assert_eq!(
            fixture.expected.schema.len(),
            fixture.expected.rows.first().map(|r| r.len()).unwrap_or(0),
            "fixture {} expected schema/row length mismatch",
            fixture.name
        );
    }

    // Create SparkSession and DataFrame from input
    let spark = SparkSession::builder()
        .app_name("parity_test")
        .get_or_create();
    let df = create_df_from_input(&spark, &fixture.input)?;

    // Create right DataFrame for join fixtures
    let right_df = fixture
        .right_input
        .as_ref()
        .map(|ri| create_df_from_input(&spark, ri))
        .transpose()?;

    // Apply operations
    let result_df = match apply_operations(df, right_df, &fixture.operations, fixture.mock_dates) {
        Ok(df) => df,
        Err(_e) if fixture.expect_error => return Ok(()),
        Err(e) => return Err(e),
    };

    // Collect and compare results
    let (actual_schema, actual_rows) = match collect_to_simple_format(&result_df) {
        Ok(v) => v,
        Err(_e) if fixture.expect_error => return Ok(()),
        Err(e) => return Err(e),
    };

    if fixture.expect_error {
        return Err(PolarsError::ComputeError(
            format!(
                "fixture {} has expect_error=true but execution succeeded (expected failure)",
                fixture.name
            )
            .into(),
        ));
    }

    // Check if operations include orderBy (for comparison strategy)
    let has_order_by = fixture
        .operations
        .iter()
        .any(|op| matches!(op, Operation::OrderBy { .. }));
    let is_join_fixture = fixture.right_input.is_some();

    assert_schema_eq(
        &actual_schema,
        &fixture.expected.schema,
        &fixture.name,
        is_join_fixture,
    )?;
    assert_rows_eq(
        &actual_rows,
        &fixture.expected.rows,
        has_order_by,
        &fixture.name,
        Some(&fixture.expected.schema),
    )?;

    Ok(())
}

/// Build a DataFrame from the JSON input section using SparkSession::create_dataframe.
///
/// For the first parity slice we support only a small subset of types:
/// - `int` / `bigint` → `i64`
/// - `string`         → UTF-8
///
/// If `file_source` is present, reads from a temporary file instead of in-memory data.
/// If `table_source` is present, creates df from rows, registers temp view, then reads via table().
fn create_df_from_input(
    spark: &SparkSession,
    input: &InputSection,
) -> Result<DataFrame, PolarsError> {
    // Check if we have a table source (spark.read.table)
    if let Some(ref table_source) = input.table_source {
        let df = create_df_from_input_direct(input)?;
        spark.create_or_replace_temp_view(&table_source.view_name, df);
        return spark.read().table(&table_source.view_name);
    }

    // Check if we have a file source
    if let Some(ref file_source) = input.file_source {
        return create_df_from_file_source(spark, file_source, input);
    }

    // Convert input to (i64, i64, String) tuples for create_dataframe
    // This assumes the first two columns are int-like and third is string
    // Only use create_dataframe if the pattern matches exactly
    if input.schema.len() == 3 {
        let type0 = input.schema[0].r#type.as_str();
        let type1 = input.schema[1].r#type.as_str();
        let type2 = input.schema[2].r#type.as_str();

        // Check if pattern matches (int, int, string)
        let is_int_int_string = (type0 == "int" || type0 == "bigint" || type0 == "long")
            && (type1 == "int" || type1 == "bigint" || type1 == "long")
            && (type2 == "string" || type2 == "str" || type2 == "varchar");

        if is_int_int_string {
            let mut tuples: Vec<(i64, i64, String)> = Vec::new();
            for row in &input.rows {
                let v0 = row.first().and_then(|v| v.as_i64()).unwrap_or(0);
                let v1 = row.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
                let v2 = row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "".to_string());
                tuples.push((v0, v1, v2));
            }
            let col_names: Vec<&str> = input.schema.iter().map(|s| s.name.as_str()).collect();
            return spark.create_dataframe(tuples, col_names);
        }
    }

    // Fallback to direct Polars construction for non-matching patterns
    create_df_from_input_direct(input)
}

/// Create a DataFrame from a file source by writing content to a temp file and reading it.
fn create_df_from_file_source(
    spark: &SparkSession,
    file_source: &FileSource,
    input: &InputSection,
) -> Result<DataFrame, PolarsError> {
    use std::io::Write;

    // Create a temporary file
    let temp_dir = std::env::temp_dir();
    let extension = match file_source.format.as_str() {
        "csv" => "csv",
        "parquet" => "parquet",
        "json" => "json",
        _ => {
            return Err(PolarsError::ComputeError(
                format!("unsupported file format: {}", file_source.format).into(),
            ))
        }
    };
    static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);
    let n = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    let temp_path = temp_dir.join(format!(
        "robin_sparkless_test_{}_{}.{}",
        std::process::id(),
        n,
        extension
    ));

    // Write content to temp file
    {
        let mut file = std::fs::File::create(&temp_path).map_err(|e| {
            PolarsError::ComputeError(format!("failed to create temp file: {e}").into())
        })?;
        file.write_all(file_source.content.as_bytes())
            .map_err(|e| {
                PolarsError::ComputeError(format!("failed to write temp file: {e}").into())
            })?;
    }

    // Build reader with optional options
    let mut reader = spark.read();
    if let Some(ref opts) = file_source.reader_options {
        for (k, v) in opts.iter() {
            reader = reader.option(k.clone(), v.clone());
        }
    }

    // Read the file using the appropriate reader
    let df = match file_source.format.as_str() {
        "csv" => reader.csv(&temp_path)?,
        "parquet" => {
            // For Parquet, use the input.rows data (what PySpark actually read) to create
            // a DataFrame, write it as Parquet, then read it back.
            // This ensures we test the Parquet reader with the same data PySpark saw.
            use polars::prelude::*;

            // Create DataFrame from input.rows (this is what PySpark read from Parquet)
            let input_df = create_df_from_input_direct(input)?;
            let pl_df = input_df.collect()?;

            // Write to Parquet using Polars ParquetWriter
            let mut df_to_write = (*pl_df).clone();
            {
                let mut file = std::fs::File::create(&temp_path).map_err(|e| {
                    PolarsError::ComputeError(format!("failed to create Parquet file: {e}").into())
                })?;

                // Use ParquetWriter from prelude
                use polars::prelude::ParquetWriter;
                ParquetWriter::new(&mut file)
                    .finish(&mut df_to_write)
                    .map_err(|e| {
                        PolarsError::ComputeError(format!("failed to write Parquet: {e}").into())
                    })?;
            }

            // Now read the Parquet file we just created (with optional reader options)
            reader.parquet(&temp_path)?
        }
        "json" => reader.json(&temp_path)?,
        _ => {
            return Err(PolarsError::ComputeError(
                format!("unsupported file format: {}", file_source.format).into(),
            ))
        }
    };

    // Clean up temp file
    std::fs::remove_file(&temp_path).ok();

    Ok(df)
}

/// Fallback: Build a Polars-backed `DataFrame` directly from the JSON input section.
fn create_df_from_input_direct(input: &InputSection) -> Result<DataFrame, PolarsError> {
    let mut cols: Vec<Series> = Vec::with_capacity(input.schema.len());

    for (col_idx, spec) in input.schema.iter().enumerate() {
        match spec.r#type.as_str() {
            "int" | "bigint" | "long" => {
                let mut vals: Vec<Option<i64>> = Vec::with_capacity(input.rows.len());
                for row in &input.rows {
                    let v = row.get(col_idx).cloned().unwrap_or(Value::Null);
                    let opt = match v {
                        Value::Number(n) => n.as_i64(),
                        Value::Null => None,
                        _ => None,
                    };
                    vals.push(opt);
                }
                cols.push(Series::new(spec.name.clone().into(), vals));
            }
            "double" | "float" | "double_precision" => {
                let mut vals: Vec<Option<f64>> = Vec::with_capacity(input.rows.len());
                for row in &input.rows {
                    let v = row.get(col_idx).cloned().unwrap_or(Value::Null);
                    let opt = match v {
                        Value::Number(n) => n.as_f64(),
                        Value::Null => None,
                        _ => None,
                    };
                    vals.push(opt);
                }
                cols.push(Series::new(spec.name.clone().into(), vals));
            }
            "string" | "str" | "varchar" => {
                let mut vals: Vec<Option<String>> = Vec::with_capacity(input.rows.len());
                for row in &input.rows {
                    let v = row.get(col_idx).cloned().unwrap_or(Value::Null);
                    let opt = match v {
                        Value::String(s) => Some(s),
                        Value::Null => None,
                        other => Some(other.to_string()),
                    };
                    vals.push(opt);
                }
                cols.push(Series::new(spec.name.clone().into(), vals));
            }
            "boolean" | "bool" => {
                let mut vals: Vec<Option<bool>> = Vec::with_capacity(input.rows.len());
                for row in &input.rows {
                    let v = row.get(col_idx).cloned().unwrap_or(Value::Null);
                    let opt = match v {
                        Value::Bool(b) => Some(b),
                        Value::Null => None,
                        _ => None,
                    };
                    vals.push(opt);
                }
                cols.push(Series::new(spec.name.clone().into(), vals));
            }
            "date" => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                    .ok_or_else(|| PolarsError::ComputeError("invalid epoch date".into()))?;
                let mut vals: Vec<Option<i32>> = Vec::with_capacity(input.rows.len());
                for row in &input.rows {
                    let v = row.get(col_idx).cloned().unwrap_or(Value::Null);
                    let opt = match v {
                        Value::String(s) => NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                            .ok()
                            .map(|d| (d - epoch).num_days() as i32),
                        Value::Null => None,
                        _ => None,
                    };
                    vals.push(opt);
                }
                let s = Series::new(spec.name.clone().into(), vals);
                cols.push(
                    s.cast(&DataType::Date)
                        .map_err(|e| PolarsError::ComputeError(format!("date cast: {e}").into()))?,
                );
            }
            "timestamp" | "datetime" | "timestamp_ntz" => {
                let mut vals: Vec<Option<i64>> = Vec::with_capacity(input.rows.len());
                for row in &input.rows {
                    let v = row.get(col_idx).cloned().unwrap_or(Value::Null);
                    let opt = match v {
                        Value::String(s) => {
                            // ISO 8601: allow with or without fractional seconds
                            let parsed = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f")
                                .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S"))
                                .or_else(|_| {
                                    NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                                        .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
                                });
                            parsed.ok().map(|dt| dt.and_utc().timestamp_micros())
                        }
                        Value::Number(n) => n.as_i64(),
                        Value::Null => None,
                        _ => None,
                    };
                    vals.push(opt);
                }
                let s = Series::new(spec.name.clone().into(), vals);
                cols.push(
                    s.cast(&DataType::Datetime(TimeUnit::Microseconds, None))
                        .map_err(|e| {
                            PolarsError::ComputeError(format!("datetime cast: {e}").into())
                        })?,
                );
            }
            other => {
                return Err(PolarsError::ComputeError(
                    format!(
                        "unsupported type in test fixture: {} (column '{}')",
                        other, spec.name
                    )
                    .into(),
                ));
            }
        }
    }

    let pl_df = PlDataFrame::new(cols.iter().map(|s| s.clone().into()).collect())?;
    Ok(DataFrame::from_polars(pl_df))
}

/// Apply the first parity-slice operations (filter + select + orderBy + groupBy + agg + join).
///
/// - `filter` supports very simple expressions of the form:
///   - `col('age') > 30`
///   - `col(\"age\") >= 10`
/// - `select` takes explicit column names.
/// - `orderBy` sorts by columns.
/// - `groupBy` creates a GroupedData (must be followed by `agg`).
/// - `agg` applies aggregations to GroupedData and returns a DataFrame.
/// - `join` joins with the right DataFrame (requires right_input in fixture).
fn apply_operations(
    mut df: DataFrame,
    mut right: Option<DataFrame>,
    ops: &[Operation],
    mock_dates: bool,
) -> Result<DataFrame, PolarsError> {
    use robin_sparkless::GroupedData;

    let mut grouped: Option<GroupedData> = None;

    for op in ops {
        match op {
            Operation::Filter { expr } => {
                // Filter must be applied before grouping
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "filter cannot be applied after groupBy".into(),
                    ));
                }
                // Pass df so column names in the expression can be resolved (case-insensitive)
                let predicate = parse_simple_filter_expr(expr, Some(&df)).map_err(|e| {
                    PolarsError::ComputeError(
                        format!("failed to parse filter expr '{expr}': {e}").into(),
                    )
                })?;
                df = df.filter(predicate)?;
            }
            Operation::Select { columns } => {
                // Select must be applied before grouping
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "select cannot be applied after groupBy".into(),
                    ));
                }
                let cols: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                df = df.select(cols)?;
            }
            Operation::OrderBy {
                columns,
                ascending,
                nulls_first,
            } => {
                if let Some(ref _gd) = grouped {
                    return Err(PolarsError::ComputeError(
                        "orderBy cannot be applied to GroupedData, must aggregate first".into(),
                    ));
                }
                if let Some(nf) = nulls_first {
                    use robin_sparkless::{
                        asc_nulls_first, asc_nulls_last, col, desc_nulls_first, desc_nulls_last,
                    };
                    let mut sort_orders = Vec::with_capacity(columns.len());
                    for (i, col_name) in columns.iter().enumerate() {
                        let asc = ascending.get(i).copied().unwrap_or(true);
                        let nf_val = nf.get(i).copied().unwrap_or(true);
                        let c = col(col_name);
                        let order = if asc {
                            if nf_val {
                                asc_nulls_first(&c)
                            } else {
                                asc_nulls_last(&c)
                            }
                        } else if nf_val {
                            desc_nulls_first(&c)
                        } else {
                            desc_nulls_last(&c)
                        };
                        sort_orders.push(order);
                    }
                    df = df.order_by_exprs(sort_orders)?;
                } else {
                    let cols: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                    df = df.order_by(cols, ascending.clone())?;
                }
            }
            Operation::GroupBy { columns } => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "nested groupBy not supported".into(),
                    ));
                }
                let cols: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                grouped = Some(df.group_by(cols)?);
            }
            Operation::Agg { aggregations } => {
                // Support global aggregation when no preceding groupBy (empty group = whole table)
                let was_global_agg = grouped.is_none();
                let gd = if let Some(g) = grouped.take() {
                    g
                } else {
                    // Global agg: add literal column, group by it, then drop after agg
                    df = df.with_column_expr(
                        "_gb_global",
                        Expr::Literal(polars::prelude::LiteralValue::Int64(1)),
                    )?;
                    df.group_by(vec!["_gb_global"])?
                };

                // Build Vec<Expr> for count, sum, avg, min, max (single or multiple)
                let mut agg_exprs: Vec<Expr> = Vec::with_capacity(aggregations.len());
                for agg_spec in aggregations {
                    let alias = if agg_spec.alias.is_empty() {
                        None
                    } else {
                        Some(agg_spec.alias.as_str())
                    };
                    let expr = match agg_spec.func.as_str() {
                        "count" => {
                            let e = len();
                            match alias {
                                Some(a) => e.alias(a),
                                None => e.alias("count"),
                            }
                        }
                        "sum" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "sum aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name).sum();
                            let name: String = alias
                                .map(String::from)
                                .filter(|a| !a.is_empty())
                                .unwrap_or_else(|| format!("sum({col_name})"));
                            e.alias(&name)
                        }
                        "avg" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "avg aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name).mean();
                            let name: String = alias
                                .map(String::from)
                                .filter(|a| !a.is_empty())
                                .unwrap_or_else(|| format!("avg({col_name})"));
                            e.alias(&name)
                        }
                        "min" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "min aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name).min();
                            let name: String = alias
                                .map(String::from)
                                .filter(|a| !a.is_empty())
                                .unwrap_or_else(|| format!("min({col_name})"));
                            e.alias(&name)
                        }
                        "max" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "max aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name).max();
                            let name: String = alias
                                .map(String::from)
                                .filter(|a| !a.is_empty())
                                .unwrap_or_else(|| format!("max({col_name})"));
                            e.alias(&name)
                        }
                        "stddev" | "stddev_samp" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "stddev aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name).std(1);
                            let name: String = alias
                                .map(String::from)
                                .filter(|a| !a.is_empty())
                                .unwrap_or_else(|| format!("stddev({col_name})"));
                            e.alias(&name)
                        }
                        "stddev_pop" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "stddev_pop aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name).std(0);
                            let name: String = alias
                                .filter(|a| !a.is_empty())
                                .map(String::from)
                                .unwrap_or_else(|| format!("stddev_pop({col_name})"));
                            e.alias(&name)
                        }
                        "variance" | "var_samp" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "variance aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name).var(1);
                            let name: String = alias
                                .map(String::from)
                                .filter(|a| !a.is_empty())
                                .unwrap_or_else(|| format!("variance({col_name})"));
                            e.alias(&name)
                        }
                        "count_distinct" | "countDistinct" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "count_distinct aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name)
                                .n_unique()
                                .cast(polars::prelude::DataType::Int64);
                            let name: String = alias
                                .map(String::from)
                                .filter(|a| !a.is_empty())
                                .unwrap_or_else(|| format!("count_distinct({col_name})"));
                            e.alias(&name)
                        }
                        "first" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "first aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name).first();
                            let name: String = alias
                                .map(String::from)
                                .filter(|a| !a.is_empty())
                                .unwrap_or_else(|| format!("first({col_name})"));
                            e.alias(&name)
                        }
                        "last" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "last aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name).last();
                            let name: String = alias
                                .map(String::from)
                                .filter(|a| !a.is_empty())
                                .unwrap_or_else(|| format!("last({col_name})"));
                            e.alias(&name)
                        }
                        "approx_count_distinct" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "approx_count_distinct aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name)
                                .n_unique()
                                .cast(polars::prelude::DataType::Int64);
                            let name: String = alias
                                .map(String::from)
                                .filter(|a| !a.is_empty())
                                .unwrap_or_else(|| format!("approx_count_distinct({col_name})"));
                            e.alias(&name)
                        }
                        "any_value" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "any_value aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name).first();
                            let name: String = alias
                                .map(String::from)
                                .filter(|a| !a.is_empty())
                                .unwrap_or_else(|| format!("any_value({col_name})"));
                            e.alias(&name)
                        }
                        "bool_and" | "every" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "bool_and aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name).all(true);
                            let name: String = alias
                                .filter(|a| !a.is_empty())
                                .map(String::from)
                                .unwrap_or_else(|| format!("bool_and({col_name})"));
                            e.alias(&name)
                        }
                        "bool_or" | "some" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "bool_or aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name).any(true);
                            let name: String = alias
                                .filter(|a| !a.is_empty())
                                .map(String::from)
                                .unwrap_or_else(|| format!("bool_or({col_name})"));
                            e.alias(&name)
                        }
                        "count_if" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "count_if aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name).cast(polars::prelude::DataType::Int64).sum();
                            let name: String = alias
                                .filter(|a| !a.is_empty())
                                .map(String::from)
                                .unwrap_or_else(|| format!("count_if({col_name})"));
                            e.alias(&name)
                        }
                        "var_pop" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "var_pop aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name).var(0);
                            let name: String = alias
                                .filter(|a| !a.is_empty())
                                .map(String::from)
                                .unwrap_or_else(|| format!("var_pop({col_name})"));
                            e.alias(&name)
                        }
                        "median" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "median aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name).quantile(
                                polars::prelude::lit(0.5),
                                polars::prelude::QuantileMethod::Linear,
                            );
                            let name: String = alias
                                .filter(|a| !a.is_empty())
                                .map(String::from)
                                .unwrap_or_else(|| format!("median({col_name})"));
                            e.alias(&name)
                        }
                        "mode" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "mode aggregation requires column name".into(),
                                )
                            })?;
                            let vc = col(col_name).value_counts(true, false, "count", false);
                            let e = vc.first().struct_().field_by_index(0);
                            let name: String = alias
                                .filter(|a| !a.is_empty())
                                .map(String::from)
                                .unwrap_or_else(|| format!("mode({col_name})"));
                            e.alias(&name)
                        }
                        "try_sum" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "try_sum aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name).sum();
                            let name: String = alias
                                .filter(|a| !a.is_empty())
                                .map(String::from)
                                .unwrap_or_else(|| format!("try_sum({col_name})"));
                            e.alias(&name)
                        }
                        "try_avg" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "try_avg aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name).mean();
                            let name: String = alias
                                .filter(|a| !a.is_empty())
                                .map(String::from)
                                .unwrap_or_else(|| format!("try_avg({col_name})"));
                            e.alias(&name)
                        }
                        "percentile" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "percentile aggregation requires column name".into(),
                                )
                            })?;
                            let p = agg_spec.percentile.unwrap_or(0.5);
                            let e = col(col_name).quantile(
                                polars::prelude::lit(p),
                                polars::prelude::QuantileMethod::Linear,
                            );
                            let name: String = alias
                                .filter(|a| !a.is_empty())
                                .map(String::from)
                                .unwrap_or_else(|| format!("percentile({col_name}, {p})"));
                            e.alias(&name)
                        }
                        "approx_percentile" | "percentile_approx" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "approx_percentile/percentile_approx aggregation requires column name".into(),
                                )
                            })?;
                            let p = agg_spec.percentile.unwrap_or(0.5);
                            let e = col(col_name).quantile(
                                polars::prelude::lit(p),
                                polars::prelude::QuantileMethod::Linear,
                            );
                            let name: String = alias
                                .filter(|a| !a.is_empty())
                                .map(String::from)
                                .unwrap_or_else(|| format!("approx_percentile({col_name}, {p})"));
                            e.alias(&name)
                        }
                        "product" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "product aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name)
                                .product()
                                .cast(polars::prelude::DataType::Float64);
                            let name: String = alias
                                .map(String::from)
                                .filter(|a| !a.is_empty())
                                .unwrap_or_else(|| format!("product({col_name})"));
                            e.alias(&name)
                        }
                        "collect_list" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "collect_list aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name).implode();
                            let name: String = alias
                                .filter(|a| !a.is_empty())
                                .map(String::from)
                                .unwrap_or_else(|| format!("collect_list({col_name})"));
                            e.alias(&name)
                        }
                        "collect_set" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "collect_set aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name).unique().implode();
                            let name: String = alias
                                .filter(|a| !a.is_empty())
                                .map(String::from)
                                .unwrap_or_else(|| format!("collect_set({col_name})"));
                            e.alias(&name)
                        }
                        "max_by" => {
                            let v_col = agg_spec.value_column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "max_by aggregation requires value_column and ord_column"
                                        .into(),
                                )
                            })?;
                            let o_col = agg_spec.ord_column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "max_by aggregation requires value_column and ord_column"
                                        .into(),
                                )
                            })?;
                            let st = polars::prelude::as_struct(vec![
                                col(o_col).alias("_ord"),
                                col(v_col).alias("_val"),
                            ]);
                            let e = st
                                .sort(
                                    polars::prelude::SortOptions::default()
                                        .with_order_descending(true),
                                )
                                .first()
                                .struct_()
                                .field_by_name("_val");
                            let name: String = alias
                                .filter(|a| !a.is_empty())
                                .map(String::from)
                                .unwrap_or_else(|| format!("max_by({v_col}, {o_col})"));
                            e.alias(&name)
                        }
                        "min_by" => {
                            let v_col = agg_spec.value_column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "min_by aggregation requires value_column and ord_column"
                                        .into(),
                                )
                            })?;
                            let o_col = agg_spec.ord_column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "min_by aggregation requires value_column and ord_column"
                                        .into(),
                                )
                            })?;
                            let st = polars::prelude::as_struct(vec![
                                col(o_col).alias("_ord"),
                                col(v_col).alias("_val"),
                            ]);
                            let e = st
                                .sort(polars::prelude::SortOptions::default())
                                .first()
                                .struct_()
                                .field_by_name("_val");
                            let name: String = alias
                                .filter(|a| !a.is_empty())
                                .map(String::from)
                                .unwrap_or_else(|| format!("min_by({v_col}, {o_col})"));
                            e.alias(&name)
                        }
                        "covar_pop" => {
                            let c1 = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "covar_pop aggregation requires column and column2".into(),
                                )
                            })?;
                            let c2 = agg_spec.column2.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "covar_pop aggregation requires column and column2".into(),
                                )
                            })?;
                            let e = robin_sparkless::covar_pop_expr(c1, c2);
                            let name: String = alias
                                .filter(|a| !a.is_empty())
                                .map(String::from)
                                .unwrap_or_else(|| format!("covar_pop({c1}, {c2})"));
                            e.alias(&name)
                        }
                        "covar_samp" => {
                            let c1 = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "covar_samp aggregation requires column and column2".into(),
                                )
                            })?;
                            let c2 = agg_spec.column2.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "covar_samp aggregation requires column and column2".into(),
                                )
                            })?;
                            let e = robin_sparkless::covar_samp_expr(c1, c2);
                            let name: String = alias
                                .filter(|a| !a.is_empty())
                                .map(String::from)
                                .unwrap_or_else(|| format!("covar_samp({c1}, {c2})"));
                            e.alias(&name)
                        }
                        "corr" => {
                            let c1 = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "corr aggregation requires column and column2".into(),
                                )
                            })?;
                            let c2 = agg_spec.column2.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "corr aggregation requires column and column2".into(),
                                )
                            })?;
                            let e = robin_sparkless::corr_expr(c1, c2);
                            let name: String = alias
                                .filter(|a| !a.is_empty())
                                .map(String::from)
                                .unwrap_or_else(|| format!("corr({c1}, {c2})"));
                            e.alias(&name)
                        }
                        "kurtosis" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "kurtosis aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name)
                                .cast(polars::prelude::DataType::Float64)
                                .kurtosis(true, true);
                            let name: String = alias
                                .filter(|a| !a.is_empty())
                                .map(String::from)
                                .unwrap_or_else(|| format!("kurtosis({col_name})"));
                            e.alias(&name)
                        }
                        "skewness" => {
                            let col_name = agg_spec.column.as_ref().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "skewness aggregation requires column name".into(),
                                )
                            })?;
                            let e = col(col_name)
                                .cast(polars::prelude::DataType::Float64)
                                .skew(true);
                            let name: String = alias
                                .filter(|a| !a.is_empty())
                                .map(String::from)
                                .unwrap_or_else(|| format!("skewness({col_name})"));
                            e.alias(&name)
                        }
                        other => {
                            return Err(PolarsError::ComputeError(
                                format!("unsupported aggregation function: {other}").into(),
                            ));
                        }
                    };
                    agg_exprs.push(expr);
                }
                df = gd.agg(agg_exprs)?;
                if was_global_agg {
                    df = df.drop(vec!["_gb_global"])?;
                }
                grouped = None; // Aggregation consumes the GroupedData
            }
            Operation::Join { on, how } => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "join cannot be applied after groupBy".into(),
                    ));
                }
                let right_df = right.take().ok_or_else(|| {
                    PolarsError::ComputeError("join requires right_input in fixture".into())
                })?;
                let join_type = match how.as_str() {
                    "inner" => JoinType::Inner,
                    "left" => JoinType::Left,
                    "right" => JoinType::Right,
                    "outer" | "full" => JoinType::Outer,
                    "left_semi" | "semi" => JoinType::LeftSemi,
                    "left_anti" | "anti" => JoinType::LeftAnti,
                    other => {
                        return Err(PolarsError::ComputeError(
                            format!("unsupported join type: {other}").into(),
                        ));
                    }
                };
                let on_refs: Vec<&str> = on.iter().map(|s| s.as_str()).collect();
                df = df.join(&right_df, on_refs, join_type)?;
            }
            Operation::WithColumn { column, expr } => {
                // Parse the expression and apply withColumn
                // For now, support simple expressions like when(), coalesce()
                let parsed_expr = parse_with_column_expr(expr, mock_dates).map_err(|e| {
                    PolarsError::ComputeError(
                        format!("failed to parse withColumn expr '{expr}': {e}").into(),
                    )
                })?;
                df = df.with_column_expr(column, parsed_expr)?;
            }
            Operation::Window {
                column: col_name,
                func,
                partition_by,
                order_by,
                value_column,
                n,
            } => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "window cannot be applied after groupBy".into(),
                    ));
                }
                let partition_refs: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
                let partition_exprs: Vec<Expr> = partition_refs.iter().map(|s| col(*s)).collect();
                let order_col = order_by
                    .as_ref()
                    .and_then(|ob| ob.first())
                    .map(|o| o.col.as_str())
                    .unwrap_or_else(|| partition_refs.first().copied().unwrap_or(""));
                let descending = order_by
                    .as_ref()
                    .and_then(|ob| ob.first())
                    .map(|o| !o.asc)
                    .unwrap_or(false);

                match func.as_str() {
                    "percent_rank" => {
                        df = df.with_column_expr(
                            "_pr_rank",
                            robin_sparkless::col(order_col)
                                .rank(descending)
                                .over(&partition_refs)
                                .into_expr(),
                        )?;
                        df = df.with_column_expr(
                            "_pr_count",
                            col(order_col).count().over(partition_exprs.clone()),
                        )?;
                        df = df.with_column_expr(
                            col_name,
                            (col("_pr_rank") - lit(1i64)).cast(DataType::Float64)
                                / (col("_pr_count") - lit(1i64)).cast(DataType::Float64),
                        )?;
                        df = df.drop(vec!["_pr_rank", "_pr_count"])?;
                    }
                    "cume_dist" => {
                        df = df.with_column_expr(
                            "_cd_rn",
                            robin_sparkless::col(order_col)
                                .row_number(descending)
                                .over(&partition_refs)
                                .into_expr(),
                        )?;
                        df = df.with_column_expr(
                            "_cd_count",
                            col(order_col).count().over(partition_exprs.clone()),
                        )?;
                        df = df.with_column_expr(
                            col_name,
                            col("_cd_rn").cast(DataType::Float64)
                                / col("_cd_count").cast(DataType::Float64),
                        )?;
                        df = df.drop(vec!["_cd_rn", "_cd_count"])?;
                    }
                    "ntile" => {
                        let n_buckets = n.unwrap_or(4) as i64;
                        df = df.with_column_expr(
                            "_nt_rank",
                            robin_sparkless::col(order_col)
                                .row_number(descending)
                                .over(&partition_refs)
                                .into_expr(),
                        )?;
                        df = df.with_column_expr(
                            "_nt_count",
                            col(order_col).count().over(partition_exprs.clone()),
                        )?;
                        // PySpark ntile: floor((rank-1)*n/count)+1, clamped to 1..n (approximately equal buckets).
                        let nt_quot = (col("_nt_rank") - lit(1i64)) * lit(n_buckets);
                        let nt_quot_f = nt_quot.cast(DataType::Float64)
                            / col("_nt_count").cast(DataType::Float64);
                        df = df.with_column_expr(
                            col_name,
                            (nt_quot_f.floor() + lit(1.0))
                                .clip(lit(1.0), lit(n_buckets as f64))
                                .cast(DataType::Int32),
                        )?;
                        df = df.drop(vec!["_nt_rank", "_nt_count"])?;
                    }
                    "nth_value" => {
                        let val_col = value_column.as_deref().unwrap_or(order_col);
                        let n_val = n.unwrap_or(1);
                        // PySpark default frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
                        // nth_value = value at rank n in the frame, or null if current row < n.
                        df = df.with_column_expr(
                            "_nv_rn",
                            robin_sparkless::col(order_col)
                                .row_number(descending)
                                .over(&partition_refs)
                                .into_expr(),
                        )?;
                        df = df.with_column_expr(
                            "_nv_val",
                            polars::prelude::when(col("_nv_rn").eq(lit(n_val)))
                                .then(robin_sparkless::col(val_col).into_expr())
                                .otherwise(Expr::Literal(polars::prelude::LiteralValue::Null))
                                .max()
                                .over(partition_exprs.clone()),
                        )?;
                        df = df.with_column_expr(
                            col_name,
                            polars::prelude::when(col("_nv_rn").gt_eq(lit(n_val)))
                                .then(col("_nv_val"))
                                .otherwise(Expr::Literal(polars::prelude::LiteralValue::Null)),
                        )?;
                        df = df.drop(vec!["_nv_rn", "_nv_val"])?;
                    }
                    _ => {
                        let window_expr = match func.as_str() {
                            "row_number" => robin_sparkless::col(order_col)
                                .row_number(descending)
                                .over(&partition_refs),
                            "rank" => robin_sparkless::col(order_col)
                                .rank(descending)
                                .over(&partition_refs),
                            "dense_rank" => robin_sparkless::col(order_col)
                                .dense_rank(descending)
                                .over(&partition_refs),
                            "lag" => {
                                let val_col = value_column.as_deref().unwrap_or(order_col);
                                robin_sparkless::col(val_col).lag(1).over(&partition_refs)
                            }
                            "lead" => {
                                let val_col = value_column.as_deref().unwrap_or(order_col);
                                robin_sparkless::col(val_col).lead(1).over(&partition_refs)
                            }
                            "first_value" => {
                                let val_col = value_column.as_deref().unwrap_or(order_col);
                                robin_sparkless::col(val_col)
                                    .first_value()
                                    .over(&partition_refs)
                            }
                            "last_value" => {
                                let val_col = value_column.as_deref().unwrap_or(order_col);
                                // PySpark default frame is ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW;
                                // last_value in that frame is the value at the current row (identity).
                                robin_sparkless::col(val_col)
                            }
                            other => {
                                return Err(PolarsError::ComputeError(
                                    format!("unsupported window function: {other}").into(),
                                ));
                            }
                        };
                        df = df.with_column_expr(col_name, window_expr.into_expr())?;
                    }
                }
            }
            Operation::Union {} => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "union cannot be applied after groupBy".into(),
                    ));
                }
                let right_df = right.take().ok_or_else(|| {
                    PolarsError::ComputeError("union requires right_input in fixture".into())
                })?;
                df = df.union(&right_df)?;
            }
            Operation::UnionByName {} => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "unionByName cannot be applied after groupBy".into(),
                    ));
                }
                let right_df = right.take().ok_or_else(|| {
                    PolarsError::ComputeError("unionByName requires right_input in fixture".into())
                })?;
                df = df.union_by_name(&right_df)?;
            }
            Operation::Distinct { subset } => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "distinct cannot be applied after groupBy".into(),
                    ));
                }
                let subset_refs: Option<Vec<&str>> = subset
                    .as_ref()
                    .map(|s| s.iter().map(|x| x.as_str()).collect());
                df = df.distinct(subset_refs)?;
            }
            Operation::Drop { columns } => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "drop cannot be applied after groupBy".into(),
                    ));
                }
                let cols: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                df = df.drop(cols)?;
            }
            Operation::Dropna { subset } => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "dropna cannot be applied after groupBy".into(),
                    ));
                }
                let subset_refs: Option<Vec<&str>> = subset
                    .as_ref()
                    .map(|s| s.iter().map(|x| x.as_str()).collect());
                df = df.dropna(subset_refs)?;
            }
            Operation::Fillna { value } => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "fillna cannot be applied after groupBy".into(),
                    ));
                }
                let fill_expr = json_value_to_lit(value).map_err(|e| {
                    PolarsError::ComputeError(format!("fillna value not supported: {e}").into())
                })?;
                df = df.fillna(fill_expr)?;
            }
            Operation::Limit { n } => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "limit cannot be applied after groupBy".into(),
                    ));
                }
                df = df.limit(*n as usize)?;
            }
            Operation::WithColumnRenamed { existing, new } => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "withColumnRenamed cannot be applied after groupBy".into(),
                    ));
                }
                df = df.with_column_renamed(existing, new)?;
            }
            Operation::Replace {
                column: col_name,
                old_value: old_val_str,
                new_value: new_val_str,
            } => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "replace cannot be applied after groupBy".into(),
                    ));
                }
                let old_expr = parse_column_or_literal(old_val_str).map_err(|e| {
                    PolarsError::ComputeError(format!("replace old_value parse error: {e}").into())
                })?;
                let new_expr = parse_column_or_literal(new_val_str).map_err(|e| {
                    PolarsError::ComputeError(format!("replace new_value parse error: {e}").into())
                })?;
                df = df.replace(col_name, old_expr, new_expr)?;
            }
            Operation::CrossJoin {} => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "crossJoin cannot be applied after groupBy".into(),
                    ));
                }
                let right_df = right.take().ok_or_else(|| {
                    PolarsError::ComputeError("crossJoin requires right_input in fixture".into())
                })?;
                df = df.cross_join(&right_df)?;
            }
            Operation::Describe {} => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "describe cannot be applied after groupBy".into(),
                    ));
                }
                df = df.describe()?;
            }
            Operation::Summary {} => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "summary cannot be applied after groupBy".into(),
                    ));
                }
                df = df.summary()?;
            }
            Operation::Subtract {} => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "subtract cannot be applied after groupBy".into(),
                    ));
                }
                let right_df = right.take().ok_or_else(|| {
                    PolarsError::ComputeError("subtract requires right_input in fixture".into())
                })?;
                df = df.subtract(&right_df)?;
            }
            Operation::Intersect {} => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "intersect cannot be applied after groupBy".into(),
                    ));
                }
                let right_df = right.take().ok_or_else(|| {
                    PolarsError::ComputeError("intersect requires right_input in fixture".into())
                })?;
                df = df.intersect(&right_df)?;
            }
            Operation::First {} => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "first cannot be applied after groupBy".into(),
                    ));
                }
                df = df.first()?;
            }
            Operation::Head { n } => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "head cannot be applied after groupBy".into(),
                    ));
                }
                df = df.head(*n as usize)?;
            }
            Operation::IsEmpty {} => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "isEmpty cannot be applied after groupBy".into(),
                    ));
                }
                // IsEmpty returns a scalar; for parity we treat as no-op (df unchanged) and compare expected rows
                // If expected has one row with one column "empty" = true/false, we could set df to a 1-row df with that.
                // For simplicity: no-op, fixture can use limit(0) + expected empty to assert empty.
            }
            Operation::Offset { n } => {
                if grouped.is_some() {
                    return Err(PolarsError::ComputeError(
                        "offset cannot be applied after groupBy".into(),
                    ));
                }
                df = df.offset(*n as usize)?;
            }
        }
    }

    if grouped.is_some() {
        return Err(PolarsError::ComputeError(
            "groupBy must be followed by agg".into(),
        ));
    }

    Ok(df)
}

/// Parse a boolean filter expression composed of comparisons combined with
/// logical operators (AND, OR, NOT, &&, ||, !) and optional parentheses.
///
/// Examples supported:
/// - `col('age') > 30`
/// - `col('age') > 30 AND col('score') < 100`
/// - `NOT col('flag') = 1`
/// - `(col('age') > 30 AND col('score') < 100) OR col('vip') = 1`
/// - `col('age') > 30 && col('score') < 100`
/// - `!col('flag') == 'N'`
fn parse_simple_filter_expr(
    src: &str,
    df_opt: Option<&robin_sparkless::DataFrame>,
) -> Result<Expr, String> {
    // For now, use the improved string-based parser that supports && and ||
    // The tokenizer above is available for future use if needed
    fn trim_outer_parens(s: &str) -> &str {
        let mut s = s.trim();
        loop {
            if !s.starts_with('(') || !s.ends_with(')') {
                return s;
            }
            // Check that the leading '(' matches the final ')'
            let mut depth = 0i32;
            let mut matched = false;
            for (i, ch) in s.char_indices() {
                match ch {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            // If the first '(' closes before the end, these aren't outer parens.
                            if i != s.len() - 1 {
                                matched = false;
                                break;
                            }
                            matched = true;
                        }
                    }
                    _ => {}
                }
            }
            if matched {
                // Strip one layer of outer parentheses and continue
                s = &s[1..s.len() - 1];
                s = s.trim();
            } else {
                return s;
            }
        }
    }

    fn find_logical_op(s: &str, patterns: &[&str]) -> Option<(usize, usize)> {
        // Look for logical operators at top level, case-insensitive,
        // ignoring anything inside quotes or nested parentheses.
        let bytes = s.as_bytes();

        let lower = s.to_ascii_lowercase();
        let lbytes = lower.as_bytes();

        // Try each pattern
        for pattern in patterns {
            let needle = pattern.to_ascii_lowercase();
            let nlen = needle.len();
            let slen = s.len();
            if slen < nlen {
                continue;
            }

            // IMPORTANT: track quote/paren state across the *entire* string.
            // We must iterate over all characters (not just up to slen-nlen), otherwise
            // state can be left "open" at the end of a scan and break subsequent scans.
            let mut depth = 0i32;
            let mut in_single = false;
            let mut in_double = false;
            for i in 0..slen {
                let ch = bytes[i] as char;
                match ch {
                    '\'' if !in_double => in_single = !in_single,
                    '"' if !in_single => in_double = !in_double,
                    '(' if !in_single && !in_double => depth += 1,
                    ')' if !in_single && !in_double => depth -= 1,
                    _ => {}
                }

                if depth == 0 && !in_single && !in_double && i + nlen <= slen {
                    // Check if we match the pattern
                    let matches = if *pattern == "AND" || *pattern == "OR" {
                        // For "AND" and "OR", require word boundaries (whitespace, parentheses, or start/end)
                        let char_before_ok = i == 0
                            || bytes[i - 1].is_ascii_whitespace()
                            || bytes[i - 1] == b'('
                            || bytes[i - 1] == b')';
                        let char_after = if i + nlen < slen {
                            bytes[i + nlen] as char
                        } else {
                            ' '
                        };
                        let char_after_ok = char_after.is_ascii_whitespace()
                            || char_after == '('
                            || char_after == ')'
                            || i + nlen >= slen;
                        char_before_ok && &lbytes[i..i + nlen] == needle.as_bytes() && char_after_ok
                    } else if *pattern == " and " || *pattern == " or " {
                        // For " and " and " or " (with spaces), exact match
                        &lbytes[i..i + nlen] == needle.as_bytes()
                    } else {
                        // For && and ||, no word boundaries needed
                        &lbytes[i..i + nlen] == needle.as_bytes()
                    };

                    if matches {
                        return Some((i, nlen));
                    }
                }
            }
        }
        None
    }

    fn parse_bool_expr(
        src: &str,
        df_opt: Option<&robin_sparkless::DataFrame>,
    ) -> Result<Expr, String> {
        let s = trim_outer_parens(src);
        let s_trimmed = s.trim();
        if s_trimmed.is_empty() {
            return Err("empty expression".to_string());
        }

        // Handle NOT / ! prefix (highest precedence)
        let lower = s_trimmed.to_ascii_lowercase();
        if lower.starts_with("not ") && s_trimmed.len() >= 4 {
            let rest = &s_trimmed[4..].trim_start();
            let inner = parse_bool_expr(rest, df_opt)?;
            return Ok(inner.not());
        }
        if s_trimmed.starts_with('!') && !s_trimmed[1..].starts_with('=') {
            let rest = &s_trimmed[1..].trim_start();
            let inner = parse_bool_expr(rest, df_opt)?;
            return Ok(inner.not());
        }

        // OR (lowest precedence) - try || first, then " or ", then OR
        // Note: We check OR before AND because AND has higher precedence
        // When we find OR at top level, we split and recurse
        if let Some((idx, len)) = find_logical_op(s_trimmed, &["||", " or ", "OR"]) {
            let left_str = s_trimmed[..idx].trim();
            let right_str = s_trimmed[idx + len..].trim();
            let lhs = parse_bool_expr(left_str, df_opt)?;
            let rhs = parse_bool_expr(right_str, df_opt)?;
            return Ok(lhs.or(rhs));
        }

        // AND (higher precedence) - try && first, then " and ", then AND
        // When we find AND at top level, we split and recurse
        if let Some((idx, len)) = find_logical_op(s_trimmed, &["&&", " and ", "AND"]) {
            let left_str = s_trimmed[..idx].trim();
            let right_str = s_trimmed[idx + len..].trim();
            let lhs = parse_bool_expr(left_str, df_opt)?;
            let rhs = parse_bool_expr(right_str, df_opt)?;
            return Ok(lhs.and(rhs));
        }

        // No logical operators at top level: treat as a single comparison
        parse_comparison_expr(s_trimmed, df_opt)
    }

    parse_bool_expr(src.trim(), df_opt)
}

/// Parser for a single comparison expression like `col('age') > 30`.
/// When df_opt is Some, column names are resolved (case-insensitive) using the DataFrame schema.
fn parse_comparison_expr(
    src: &str,
    df_opt: Option<&robin_sparkless::DataFrame>,
) -> Result<Expr, String> {
    let s = src.trim();

    // Expect something like: col('age') > 30
    let col_start = s.find("col(").ok_or("missing 'col('")?;
    let quote1 = s[col_start..]
        .find(['\'', '"'])
        .ok_or("missing opening quote for column")?
        + col_start;
    let rest = &s[quote1 + 1..];
    let quote2 = rest
        .find(['\'', '"'])
        .ok_or("missing closing quote for column")?
        + quote1
        + 1;

    let col_name_raw = &s[quote1 + 1..quote2];
    let col_name: String = if let Some(df) = df_opt {
        df.resolve_column_name(col_name_raw)
            .map_err(|e| e.to_string())?
    } else {
        col_name_raw.to_string()
    };

    // After the closing quote, find the closing paren, then expect an operator and a literal.
    let after_quote = &s[quote2 + 1..];
    let paren_end = after_quote
        .find(')')
        .ok_or("missing closing paren after column")?;
    let after_col = &after_quote[paren_end + 1..].trim();

    // Find the operator - could be ==, !=, >=, <=, >, <, =
    let op = if after_col.starts_with("==") {
        "=="
    } else if after_col.starts_with("!=") {
        "!="
    } else if after_col.starts_with(">=") {
        ">="
    } else if after_col.starts_with("<=") {
        "<="
    } else if after_col.starts_with(">") {
        ">"
    } else if after_col.starts_with("<") {
        "<"
    } else if after_col.starts_with("=") {
        "="
    } else {
        return Err(format!("unable to find operator in '{after_col}'"));
    };

    // Extract the right side after the operator, but stop at logical operators
    let right_side_full = after_col[op.len()..].trim();

    // Find where the right side ends (stop at logical operators: AND, OR, &&, ||)
    // Simple check: find the first occurrence of these patterns (case-insensitive, with word boundaries)
    let lower = right_side_full.to_ascii_lowercase();
    let mut right_side_end = right_side_full.len();

    // Check for " and ", "AND", "&&" (with word boundaries)
    for pattern in &[" and ", " and", "and ", "&&"] {
        if let Some(pos) = lower.find(pattern) {
            // Check word boundaries
            let char_before_ok =
                pos == 0 || right_side_full.as_bytes()[pos - 1].is_ascii_whitespace();
            let char_after_pos = pos + pattern.len();
            let char_after_ok = char_after_pos >= right_side_full.len()
                || right_side_full.as_bytes()[char_after_pos].is_ascii_whitespace()
                || right_side_full.as_bytes()[char_after_pos] == b'(';
            if char_before_ok && char_after_ok {
                right_side_end = right_side_end.min(pos);
            }
        }
    }

    // Check for " or ", "OR", "||" (with word boundaries)
    for pattern in &[" or ", " or", "or ", "||"] {
        if let Some(pos) = lower.find(pattern) {
            // Check word boundaries
            let char_before_ok =
                pos == 0 || right_side_full.as_bytes()[pos - 1].is_ascii_whitespace();
            let char_after_pos = pos + pattern.len();
            let char_after_ok = char_after_pos >= right_side_full.len()
                || right_side_full.as_bytes()[char_after_pos].is_ascii_whitespace()
                || right_side_full.as_bytes()[char_after_pos] == b'(';
            if char_before_ok && char_after_ok {
                right_side_end = right_side_end.min(pos);
            }
        }
    }

    let right_side = right_side_full[..right_side_end].trim();

    let c = col(col_name.as_str());

    // Check if right side is a column (col('name')) or a literal
    let right_expr_is_column = right_side.starts_with("col(");
    let right_expr = if right_expr_is_column {
        // Column-to-column comparison - will use null-aware methods
        let right_quote_start = right_side
            .find(['\'', '"'])
            .ok_or("missing quote in right column")?;
        let right_quote_end = right_side[right_quote_start + 1..]
            .find(['\'', '"'])
            .ok_or("missing closing quote")?;
        let right_col_name_raw =
            &right_side[right_quote_start + 1..right_quote_start + 1 + right_quote_end];
        let right_col_name: String = if let Some(df) = df_opt {
            df.resolve_column_name(right_col_name_raw)
                .map_err(|e| e.to_string())?
        } else {
            right_col_name_raw.to_string()
        };
        use robin_sparkless::col as robin_col;
        robin_col(right_col_name.as_str()).into_expr()
    } else if (right_side.starts_with('\'') && right_side.ends_with('\''))
        || (right_side.starts_with('"') && right_side.ends_with('"'))
    {
        // String literal - remove quotes
        let str_val = &right_side[1..right_side.len() - 1];
        lit(str_val)
    } else {
        // Try to parse as integer or float
        if let Ok(lit_val) = right_side.parse::<i64>() {
            lit(lit_val)
        } else if let Ok(lit_val) = right_side.parse::<f64>() {
            lit(lit_val)
        } else {
            return Err(format!(
                "unable to parse right side '{right_side}' as column or literal"
            ));
        }
    };

    // For type coercion, we'd need to know the column's actual type
    // For now, use Polars comparisons directly - they should handle basic type coercion
    // TODO: Add explicit type coercion using type_coercion module when column schema is available

    let expr = if right_expr_is_column {
        // Column-to-column comparison: use null-aware _pyspark methods
        use polars::prelude::DataType;
        use robin_sparkless::col as robin_col;
        let left_col = robin_col(col_name.as_str());
        let right_col = robin_sparkless::Column::from_expr(right_expr, None);

        // Pilot usage of type_coercion: coerce both sides to a common numeric type.
        // For now we assume Int64 for these test cases, which are all integer-based.
        let (left_expr_coerced, right_expr_coerced) =
            match robin_sparkless::type_coercion::coerce_for_comparison(
                left_col.expr().clone(),
                right_col.expr().clone(),
                &DataType::Int64,
                &DataType::Int64,
            ) {
                Ok((l, r)) => (l, r),
                Err(_) => (left_col.expr().clone(), right_col.expr().clone()),
            };
        let left_col = robin_sparkless::Column::from_expr(left_expr_coerced, None);
        let right_col = robin_sparkless::Column::from_expr(right_expr_coerced, None);

        match op {
            ">" => left_col.gt_pyspark(&right_col).into_expr(),
            ">=" => left_col.ge_pyspark(&right_col).into_expr(),
            "<" => left_col.lt_pyspark(&right_col).into_expr(),
            "<=" => left_col.le_pyspark(&right_col).into_expr(),
            "==" | "=" => left_col.eq_pyspark(&right_col).into_expr(),
            "!=" | "<>" => left_col.ne_pyspark(&right_col).into_expr(),
            other => return Err(format!("unsupported operator '{other}'")),
        }
    } else {
        // Column-to-literal comparison: use standard methods (Polars handles nulls in literals)
        match op {
            ">" => c.gt(right_expr),
            ">=" => c.gt_eq(right_expr),
            "<" => c.lt(right_expr),
            "<=" => c.lt_eq(right_expr),
            "==" | "=" => c.eq(right_expr),
            "!=" | "<>" => c.neq(right_expr),
            other => return Err(format!("unsupported operator '{other}'")),
        }
    };

    Ok(expr)
}

/// Extract the first argument of func_name(arg, ...) by matching parens
fn extract_first_arg<'a>(s: &'a str, prefix: &str) -> Result<&'a str, String> {
    let rest = s.strip_prefix(prefix).ok_or("prefix mismatch")?;
    let mut depth = 0u32;
    for (i, ch) in rest.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                if depth == 0 {
                    return Ok(rest[..i].trim());
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    Err("unmatched parentheses".to_string())
}

/// Extract column name from col('name') or col("name")
fn extract_col_name(s: &str) -> Result<&str, String> {
    let s = s.trim();
    if !s.starts_with("col(") {
        return Err(format!("expected col(...), got {s}"));
    }
    let content = &s[4..s.len() - 1];
    let quote = content.find(['\'', '"']).ok_or("missing quote in col()")?;
    let qchar = content.as_bytes()[quote] as char;
    let rest = &content[quote + 1..];
    let end = rest.find(qchar).ok_or("missing closing quote")?;
    Ok(&content[quote + 1..quote + 1 + end])
}

/// Parse struct field ref: col("").struct_().field_by_name("value") etc.
fn parse_struct_field_expr(s: &str) -> Result<polars::prelude::Expr, String> {
    let s = s.trim();
    let prefix = "col(\"\").struct_().field_by_name(";
    let prefix2 = "col('').struct_().field_by_name(";
    let (rest, _) = if let Some(stripped) = s.strip_prefix(prefix) {
        (stripped, prefix)
    } else if let Some(stripped) = s.strip_prefix(prefix2) {
        (stripped, prefix2)
    } else {
        return Err(format!(
            "expected col(\"\").struct_().field_by_name(...), got {s}"
        ));
    };
    let field_name = rest.trim_matches([')', '"', '\'', ' ']);
    Ok(polars::prelude::col("").struct_().field_by_name(field_name))
}

/// Parse predicate for map_filter: col("").struct_().field_by_name("value") > lit(30) etc.
fn parse_map_filter_predicate(s: &str) -> Result<polars::prelude::Expr, String> {
    let s = s.trim();
    for (op, cmp) in [
        (" > ", "gt"),
        (" < ", "lt"),
        (" >= ", "gte"),
        (" <= ", "lte"),
    ] {
        if let Some(pos) = s.find(op) {
            let (left, right) = (&s[..pos], s[pos + op.len()..].trim());
            if parse_struct_field_expr(left).is_ok() {
                let field_expr = parse_struct_field_expr(left)?;
                let right_expr = parse_column_or_literal(right)?;
                let pred = match cmp {
                    "gt" => field_expr.gt(right_expr),
                    "lt" => field_expr.lt(right_expr),
                    "gte" => field_expr.gt_eq(right_expr),
                    "lte" => field_expr.lt_eq(right_expr),
                    _ => continue,
                };
                return Ok(pred);
            }
        }
    }
    Err(format!("map_filter predicate not supported: {s}"))
}

/// Parse merge expr for zip_with: coalesce(col("").struct_().field_by_name("left"), col("").struct_().field_by_name("right"))
fn parse_zip_with_merge(s: &str) -> Result<polars::prelude::Expr, String> {
    let s = s.trim();
    if s.starts_with("coalesce(") {
        let inner = extract_first_arg(s, "coalesce(")?;
        let parts = parse_comma_separated_args(inner);
        if parts.len() >= 2 {
            let left = parse_struct_field_expr(parts[0].trim())?;
            let right = parse_struct_field_expr(parts[1].trim())?;
            return Ok(robin_sparkless::coalesce(&[
                &robin_sparkless::Column::from_expr(left, None),
                &robin_sparkless::Column::from_expr(right, None),
            ])
            .into_expr());
        }
    }
    Err(format!("zip_with merge expr not supported: {s}"))
}

/// Parse merge expr for map_zip_with: coalesce(col("").struct_().field_by_name("value1"), col("").struct_().field_by_name("value2"))
fn parse_map_zip_with_merge(s: &str) -> Result<polars::prelude::Expr, String> {
    let s = s.trim();
    if s.starts_with("coalesce(") {
        let inner = extract_first_arg(s, "coalesce(")?;
        let parts = parse_comma_separated_args(inner);
        if parts.len() >= 2 {
            let v1 = parse_struct_field_expr(parts[0].trim())?;
            let v2 = parse_struct_field_expr(parts[1].trim())?;
            return Ok(robin_sparkless::coalesce(&[
                &robin_sparkless::Column::from_expr(v1, None),
                &robin_sparkless::Column::from_expr(v2, None),
            ])
            .into_expr());
        }
    }
    Err(format!("map_zip_with merge expr not supported: {s}"))
}

/// Helper to parse comma-separated args (respecting nested parens)
fn parse_comma_separated_args(inner: &str) -> Vec<&str> {
    let mut parts: Vec<&str> = Vec::new();
    let mut start = 0;
    let mut depth = 0;
    for (i, ch) in inner.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(inner[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(inner[start..].trim());
    parts
}

/// Parse col/lit from a part string
fn parse_column_or_literal_for_concat(part: &str) -> Result<robin_sparkless::Column, String> {
    use robin_sparkless::{col, lit_i64, lit_str};
    let part = part.trim();
    if part.starts_with("col(") {
        let content = &part[4..part.len() - 1];
        let col_name = content.trim_matches(['\'', '"']);
        Ok(col(col_name))
    } else if part.starts_with("lit(") {
        let content = &part[4..part.len() - 1];
        let lit_val = content.trim_matches(['\'', '"']);
        Ok(lit_str(lit_val))
    } else if (part.starts_with('\'') && part.ends_with('\''))
        || (part.starts_with('"') && part.ends_with('"'))
    {
        let lit_val = part.trim_matches(['\'', '"']);
        Ok(lit_str(lit_val))
    } else if let Ok(num) = part.parse::<i64>() {
        Ok(lit_i64(num))
    } else {
        Err(format!("unexpected part: {part}"))
    }
}

/// Convert a JSON value to a Polars literal Expr for fillna.
fn json_value_to_lit(v: &serde_json::Value) -> Result<Expr, String> {
    use polars::prelude::LiteralValue;
    match v {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(lit(i))
            } else if let Some(f) = n.as_f64() {
                Ok(lit(f))
            } else {
                Err("unsupported number for fillna".to_string())
            }
        }
        serde_json::Value::String(s) => Ok(lit(s.as_str())),
        serde_json::Value::Bool(b) => Ok(lit(*b)),
        serde_json::Value::Null => Ok(Expr::Literal(LiteralValue::Null)),
        _ => Err("unsupported type for fillna (array/object)".to_string()),
    }
}

/// Parse expressions for withColumn operations (when, coalesce, string funcs, array funcs, etc.)
fn parse_with_column_expr(src: &str, mock_dates: bool) -> Result<Expr, String> {
    use polars::prelude::concat_list;
    use robin_sparkless::{
        abs, acos, add_months, array_append, array_compact, array_contains, array_distinct,
        array_except, array_insert, array_intersect, array_prepend, array_size, array_sum,
        array_union, arrays_overlap, arrays_zip, ascii, asin, assert_true, atan, atan2, base64,
        bin, bit_and, bit_count, bit_get, bit_length, bit_or, bit_xor, bitwise_not, bround, btrim,
        cast, cbrt, ceiling, char as rs_char, char_length, chr, coalesce, col, concat, concat_ws,
        contains, conv, cos, cosh, cot, create_map, csc, curdate, current_catalog,
        current_database, current_date, current_schema, current_timestamp, current_user, date_add,
        date_format, date_from_unix_date, date_sub, datediff, day, dayname, dayofmonth, dayofweek,
        dayofyear, degrees, e, element_at, elt, endswith, equal_null, exp, extract, factorial,
        find_in_set, format_number, format_string, from_unixtime, from_utc_timestamp, get, getbit,
        greatest, hash, hex, hour, hypot, ilike, initcap, input_file_name, instr, isin_i64,
        isin_str, isnan, isnotnull, isnull, json_array_length, last_day, lcase, least, left,
        length, like, lit_str, ln, localtimestamp, locate, log, log10, lower, lpad, make_date,
        make_timestamp, map_concat, map_contains_key, map_filter, map_from_entries, map_zip_with,
        md5, minute, monotonically_increasing_id, months_between, named_struct, nanvl, negate,
        next_day, now, nullif, nvl, nvl2, octet_length, overlay, parse_url, pi, pmod, positive,
        pow, power, quarter, radians, raise_error, rand, randn, regexp_count, regexp_extract,
        regexp_extract_all, regexp_instr, regexp_like, regexp_replace, regexp_substr, repeat,
        replace, reverse, right, rlike, rpad, sec, second, sha1, sha2, shift_left, shift_right,
        signum, sin, sinh, size, spark_partition_id, split, split_part, sqrt, startswith,
        str_to_map, struct_, substr, substring, tan, tanh, timestamp_micros, timestamp_millis,
        timestamp_seconds, timestampadd, timestampdiff, to_char, to_degrees, to_number, to_radians,
        to_timestamp, to_unix_timestamp, to_utc_timestamp, trim, trunc, try_add, try_cast,
        try_divide, try_multiply, try_subtract, try_to_number, try_to_timestamp, typeof_, ucase,
        unbase64, unhex, unix_date, unix_micros, unix_millis, unix_seconds, unix_timestamp,
        unix_timestamp_now, upper, url_decode, url_encode, user, version, weekday, weekofyear,
        when, when_then_otherwise_null, width_bucket, zip_with,
    };

    let s = src.trim();

    // Handle assert_true(col('name')) or assert_true(col('name'), 'errMsg')
    if s.starts_with("assert_true(") {
        let inner = extract_first_arg(s, "assert_true(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("assert_true needs column")?)?;
        let err_msg = parts
            .get(1)
            .map(|p| p.trim_matches(['\'', '"']).to_string());
        let c = col(col_name);
        return Ok(assert_true(&c, err_msg.as_deref()).into_expr());
    }

    // Handle raise_error('message')
    if s.starts_with("raise_error(") {
        let inner = extract_first_arg(s, "raise_error(")?;
        let msg = inner.trim_matches(['\'', '"']).to_string();
        return Ok(raise_error(&msg).into_expr());
    }

    // No-arg JVM/runtime stubs: spark_partition_id(), input_file_name(), ...
    if s == "spark_partition_id()" {
        return Ok(spark_partition_id().into_expr());
    }
    if s == "input_file_name()" {
        return Ok(input_file_name().into_expr());
    }
    if s == "monotonically_increasing_id()" {
        return Ok(monotonically_increasing_id().into_expr());
    }
    if s == "current_catalog()" {
        return Ok(current_catalog().into_expr());
    }
    if s == "current_database()" {
        return Ok(current_database().into_expr());
    }
    if s == "current_schema()" {
        return Ok(current_schema().into_expr());
    }
    if s == "current_user()" {
        return Ok(current_user().into_expr());
    }
    if s == "user()" {
        return Ok(user().into_expr());
    }

    // rand(), rand(seed), randn(), randn(seed)
    if s.starts_with("randn(") {
        let inner = extract_first_arg(s, "randn(")?;
        let seed = inner.trim().parse::<u64>().ok();
        return Ok(randn(seed).into_expr());
    }
    if s.starts_with("rand(") {
        let inner = extract_first_arg(s, "rand(")?;
        let seed = inner.trim().parse::<u64>().ok();
        return Ok(rand(seed).into_expr());
    }

    // Handle when(cond, value) two-arg or when().then().otherwise() expressions
    if s.starts_with("when(") {
        let inner = extract_first_arg(s, "when(")?;
        let parts = parse_comma_separated_args(inner);
        if parts.len() == 2 {
            // when(condition, value) -> value where condition true, null otherwise
            let condition_expr = parse_simple_filter_expr(parts[0].trim(), None)?;
            let condition_col = robin_sparkless::Column::from_expr(condition_expr, None);
            let value_expr = parse_column_or_literal(parts[1].trim())?;
            let value_col = robin_sparkless::Column::from_expr(value_expr, None);
            return Ok(when_then_otherwise_null(&condition_col, &value_col).into_expr());
        }

        // Find the condition part: when(col('age') >= 18)...
        // The condition ends at the first ")."
        let cond_end = s.find(").").ok_or("missing ). in when expression")?;
        let cond_str = &s[5..cond_end]; // Skip "when("

        // Parse condition - could be a filter expression or a comparison with columns
        let condition_expr = parse_simple_filter_expr(cond_str, None)?;
        let condition_col = robin_sparkless::Column::from_expr(condition_expr, None);

        // Check if we have .then() or just .otherwise()
        if let Some(then_pos) = s.find(").then(") {
            // when(cond).then(val).otherwise(fallback)
            let after_then = &s[then_pos + 7..]; // Skip ").then("
            let otherwise_pos = after_then
                .find(").otherwise(")
                .ok_or("missing ).otherwise() after .then()")?;

            let then_str = &after_then[..otherwise_pos];
            let after_otherwise = &after_then[otherwise_pos + 11..]; // Skip ").otherwise("
                                                                     // The otherwise value should be a simple literal or column, find the closing paren
                                                                     // For simple cases like otherwise('minor'), just find the last )
            let otherwise_end = after_otherwise
                .rfind(')')
                .ok_or("missing closing ) in otherwise()")?;
            let otherwise_str = &after_otherwise[..otherwise_end].trim();

            // Parse then and otherwise values
            let then_val = parse_column_or_literal(then_str.trim())?;
            let otherwise_val = parse_column_or_literal(otherwise_str)?;

            // Build when expression
            let then_col = robin_sparkless::Column::from_expr(then_val, None);
            let otherwise_col = robin_sparkless::Column::from_expr(otherwise_val, None);
            let when_expr = when(&condition_col)
                .then(&then_col)
                .otherwise(&otherwise_col);
            return Ok(when_expr.into_expr());
        } else if let Some(otherwise_pos) = s.find(").otherwise(") {
            // when(cond).otherwise(val) - use condition as the "then" value
            let after_otherwise = &s[otherwise_pos + 11..]; // Skip ").otherwise("
            let otherwise_end = after_otherwise
                .rfind(')')
                .ok_or("missing closing ) in otherwise()")?;
            let otherwise_str = &after_otherwise[..otherwise_end];

            let otherwise_val = parse_column_or_literal(otherwise_str)?;
            let otherwise_col = robin_sparkless::Column::from_expr(otherwise_val, None);

            // For when(cond).otherwise(val), use condition as both condition and "then"
            // This is a simplified interpretation
            let when_expr = when(&condition_col).otherwise(&otherwise_col);
            return Ok(when_expr.into_expr());
        } else {
            return Err("when expression must have .then() or .otherwise()".to_string());
        }
    }

    // Handle eqNullSafe() expressions: col('a').eqNullSafe(col('b'))
    if s.contains(".eqNullSafe(") {
        // Parse: col('value1').eqNullSafe(col('value2'))
        let eq_null_safe_pos = s.find(".eqNullSafe(").ok_or("missing .eqNullSafe(")?;
        let left_part = &s[..eq_null_safe_pos];
        let after_eq = &s[eq_null_safe_pos + 12..]; // Skip ".eqNullSafe("

        // Parse left column: col('value1')
        let left_col_name = if left_part.starts_with("col(") {
            let quote_start = left_part
                .find(['\'', '"'])
                .ok_or("missing quote in left column")?;
            let quote_end = left_part[quote_start + 1..]
                .find(['\'', '"'])
                .ok_or("missing closing quote")?;
            &left_part[quote_start + 1..quote_start + 1 + quote_end]
        } else {
            return Err("left side of eqNullSafe must be col(...)".to_string());
        };

        // Parse right column: col('value2')
        let right_part_end = after_eq
            .rfind(')')
            .ok_or("missing closing ) in eqNullSafe")?;
        let right_part = &after_eq[..right_part_end];
        let right_col_name = if right_part.starts_with("col(") {
            let quote_start = right_part
                .find(['\'', '"'])
                .ok_or("missing quote in right column")?;
            let quote_end = right_part[quote_start + 1..]
                .find(['\'', '"'])
                .ok_or("missing closing quote")?;
            &right_part[quote_start + 1..quote_start + 1 + quote_end]
        } else {
            return Err("right side of eqNullSafe must be col(...)".to_string());
        };

        let left_col = col(left_col_name);
        let right_col = col(right_col_name);
        let eq_null_safe_col = left_col.eq_null_safe(&right_col);
        return Ok(eq_null_safe_col.into_expr());
    }

    // Handle upper(col('name')) - extract arg by matching parens
    if s.starts_with("upper(") {
        let inner = extract_first_arg(s, "upper(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(upper(&c).into_expr());
    }

    // Handle lower(col('name'))
    if s.starts_with("lower(") {
        let inner = extract_first_arg(s, "lower(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(lower(&c).into_expr());
    }

    // Handle length(col('name')), char_length(...), character_length(...)
    if s.starts_with("length(") {
        let inner = extract_first_arg(s, "length(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(length(&c).into_expr());
    }
    if s.starts_with("char_length(") || s.starts_with("character_length(") {
        let prefix = if s.starts_with("char_length(") {
            "char_length("
        } else {
            "character_length("
        };
        let inner = extract_first_arg(s, prefix)?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(char_length(&c).into_expr());
    }

    // Handle abs(col('x'))
    if s.starts_with("abs(") {
        let inner = extract_first_arg(s, "abs(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(abs(&c).into_expr());
    }

    // Handle date_format(col('d'), 'yyyy-MM')
    if s.starts_with("date_format(") {
        let inner = extract_first_arg(s, "date_format(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("date_format needs column")?)?;
        let format_str = parts
            .get(1)
            .ok_or("date_format needs format string")?
            .trim()
            .trim_matches(['\'', '"'])
            .to_string();
        let c = col(col_name);
        return Ok(date_format(&c, &format_str).into_expr());
    }

    // Handle isnull(col('name')), isnotnull(col('name'))
    if s.starts_with("isnull(") {
        let inner = extract_first_arg(s, "isnull(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(isnull(&c).into_expr());
    }
    if s.starts_with("isnotnull(") {
        let inner = extract_first_arg(s, "isnotnull(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(isnotnull(&c).into_expr());
    }

    // Handle substr(col('name'), start, length) - alias for substring
    if s.starts_with("substr(") {
        let inner = extract_first_arg(s, "substr(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("substr needs column")?)?;
        let start: i64 = parts
            .get(1)
            .ok_or("substr needs start")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let length: Option<i64> = parts.get(2).and_then(|a| a.trim().parse().ok());
        let c = col(col_name);
        return Ok(substr(&c, start, length).into_expr());
    }

    // Handle power(col('name'), exp)
    if s.starts_with("power(") {
        let inner = extract_first_arg(s, "power(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("power needs column")?)?;
        let exp: i64 = parts
            .get(1)
            .ok_or("power needs exp")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let c = col(col_name);
        return Ok(power(&c, exp).into_expr());
    }

    // Handle ln(col('name'))
    if s.starts_with("ln(") {
        let inner = extract_first_arg(s, "ln(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(ln(&c).into_expr());
    }

    // Handle ceiling(col('name'))
    if s.starts_with("ceiling(") {
        let inner = extract_first_arg(s, "ceiling(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(ceiling(&c).into_expr());
    }

    // Handle bit_count(col('name'))
    if s.starts_with("bit_count(") {
        let inner = extract_first_arg(s, "bit_count(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(bit_count(&c).into_expr());
    }

    // Handle bitwise_not(col('name')) / bitwiseNOT(col('name'))
    if s.starts_with("bitwise_not(") || s.starts_with("bitwiseNOT(") {
        // Both variants have the same argument layout, so we can strip the function name generically.
        let inner = if s.starts_with("bitwise_not(") {
            extract_first_arg(s, "bitwise_not(")?
        } else {
            extract_first_arg(s, "bitwiseNOT(")?
        };
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(bitwise_not(&c).into_expr());
    }

    // Handle bit_get(col('name'), pos)
    if s.starts_with("bit_get(") {
        let inner = extract_first_arg(s, "bit_get(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("bit_get needs column")?)?;
        let pos: i64 = parts
            .get(1)
            .ok_or("bit_get needs position")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let c = col(col_name);
        return Ok(bit_get(&c, pos).into_expr());
    }

    // Handle bit_and/bit_or/bit_xor with two columns: bit_and(col('a'), col('b'))
    if s.starts_with("bit_and(") || s.starts_with("bit_or(") || s.starts_with("bit_xor(") {
        let (fn_name, prefix) = if s.starts_with("bit_and(") {
            ("bit_and", "bit_and(")
        } else if s.starts_with("bit_or(") {
            ("bit_or", "bit_or(")
        } else {
            ("bit_xor", "bit_xor(")
        };
        let inner = extract_first_arg(s, prefix)?;
        let parts = parse_comma_separated_args(inner);
        if parts.len() != 2 {
            return Err(format!("{fn_name} requires two column arguments"));
        }
        let left_name = extract_col_name(parts.first().unwrap())?;
        let right_name = extract_col_name(parts.get(1).unwrap())?;
        let left_col = col(left_name);
        let right_col = col(right_name);
        let expr = match fn_name {
            "bit_and" => bit_and(&left_col, &right_col),
            "bit_or" => bit_or(&left_col, &right_col),
            "bit_xor" => bit_xor(&left_col, &right_col),
            _ => unreachable!(),
        };
        return Ok(expr.into_expr());
    }

    // Handle lcase(col('name')), ucase(col('name'))
    if s.starts_with("lcase(") {
        let inner = extract_first_arg(s, "lcase(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(lcase(&c).into_expr());
    }
    if s.starts_with("ucase(") {
        let inner = extract_first_arg(s, "ucase(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(ucase(&c).into_expr());
    }

    // Handle dayofmonth(col('name')), day(col('name'))
    if s.starts_with("dayofmonth(") {
        let inner = extract_first_arg(s, "dayofmonth(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(dayofmonth(&c).into_expr());
    }
    if s.starts_with("day(") && !s.starts_with("dayof") {
        let inner = extract_first_arg(s, "day(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(day(&c).into_expr());
    }

    // Handle to_degrees(col('name')), toRadians(col('name')) - to_degrees/to_radians in Rust
    if s.starts_with("to_degrees(") || s.starts_with("toDegrees(") {
        let prefix = if s.starts_with("to_degrees(") {
            "to_degrees("
        } else {
            "toDegrees("
        };
        let inner = extract_first_arg(s, prefix)?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(to_degrees(&c).into_expr());
    }
    if s.starts_with("to_radians(") || s.starts_with("toRadians(") {
        let prefix = if s.starts_with("to_radians(") {
            "to_radians("
        } else {
            "toRadians("
        };
        let inner = extract_first_arg(s, prefix)?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(to_radians(&c).into_expr());
    }

    // Handle nvl2(col1, col2, col3)
    if s.starts_with("nvl2(") {
        let inner = &s[5..s.len() - 1];
        let parts = parse_comma_separated_args(inner);
        let col1 = parse_column_or_literal(parts.first().ok_or("nvl2 needs col1")?.trim())?;
        let col2 = parse_column_or_literal(parts.get(1).ok_or("nvl2 needs col2")?.trim())?;
        let col3 = parse_column_or_literal(parts.get(2).ok_or("nvl2 needs col3")?.trim())?;
        let c1 = robin_sparkless::Column::from_expr(col1, None);
        let c2 = robin_sparkless::Column::from_expr(col2, None);
        let c3 = robin_sparkless::Column::from_expr(col3, None);
        return Ok(nvl2(&c1, &c2, &c3).into_expr());
    }

    // Handle trim(col('name')), ltrim(...), rtrim(...)
    if s.starts_with("trim(") {
        let inner = extract_first_arg(s, "trim(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(trim(&c).into_expr());
    }
    if s.starts_with("ltrim(") {
        let inner = extract_first_arg(s, "ltrim(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(robin_sparkless::ltrim(&c).into_expr());
    }
    if s.starts_with("rtrim(") {
        let inner = extract_first_arg(s, "rtrim(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(robin_sparkless::rtrim(&c).into_expr());
    }

    // Handle initcap(col('name'))
    if s.starts_with("initcap(") {
        let inner = extract_first_arg(s, "initcap(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(initcap(&c).into_expr());
    }

    // Handle regexp_extract(col('name'), pattern, groupIndex)
    if s.starts_with("regexp_extract(") {
        let inner = extract_first_arg(s, "regexp_extract(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("regexp_extract needs column")?)?;
        let pattern = parts
            .get(1)
            .ok_or("regexp_extract needs pattern")?
            .trim_matches(['\'', '"']);
        let group_index: usize = parts
            .get(2)
            .ok_or("regexp_extract needs group index")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let c = col(col_name);
        return Ok(regexp_extract(&c, pattern, group_index).into_expr());
    }

    // Handle regexp_replace(col('name'), pattern, replacement)
    if s.starts_with("regexp_replace(") {
        let inner = extract_first_arg(s, "regexp_replace(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("regexp_replace needs column")?)?;
        let pattern = parts
            .get(1)
            .ok_or("regexp_replace needs pattern")?
            .trim_matches(['\'', '"']);
        let replacement = parts
            .get(2)
            .ok_or("regexp_replace needs replacement")?
            .trim_matches(['\'', '"']);
        let c = col(col_name);
        return Ok(regexp_replace(&c, pattern, replacement).into_expr());
    }

    // Handle regexp_extract_all(col('name'), pattern)
    if s.starts_with("regexp_extract_all(") {
        let inner = extract_first_arg(s, "regexp_extract_all(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("regexp_extract_all needs column")?)?;
        let pattern = parts
            .get(1)
            .ok_or("regexp_extract_all needs pattern")?
            .trim_matches(['\'', '"']);
        let c = col(col_name);
        return Ok(regexp_extract_all(&c, pattern).into_expr());
    }

    // Handle regexp_like(col('name'), pattern)
    if s.starts_with("regexp_like(") {
        let inner = extract_first_arg(s, "regexp_like(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("regexp_like needs column")?)?;
        let pattern = parts
            .get(1)
            .ok_or("regexp_like needs pattern")?
            .trim_matches(['\'', '"']);
        let c = col(col_name);
        return Ok(regexp_like(&c, pattern).into_expr());
    }

    // Handle regexp_count(col('name'), pattern)
    if s.starts_with("regexp_count(") {
        let inner = extract_first_arg(s, "regexp_count(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("regexp_count needs column")?)?;
        let pattern = parts
            .get(1)
            .ok_or("regexp_count needs pattern")?
            .trim_matches(['\'', '"']);
        let c = col(col_name);
        return Ok(regexp_count(&c, pattern).into_expr());
    }

    // Handle regexp_instr(col('name'), pattern) or regexp_instr(col('name'), pattern, idx)
    if s.starts_with("regexp_instr(") {
        let inner = extract_first_arg(s, "regexp_instr(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("regexp_instr needs column")?)?;
        let pattern = parts
            .get(1)
            .ok_or("regexp_instr needs pattern")?
            .trim_matches(['\'', '"']);
        let group_idx: Option<usize> = parts.get(2).and_then(|p| p.trim().parse().ok());
        let c = col(col_name);
        return Ok(regexp_instr(&c, pattern, group_idx).into_expr());
    }

    // Handle regexp_substr(col('name'), pattern)
    if s.starts_with("regexp_substr(") {
        let inner = extract_first_arg(s, "regexp_substr(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("regexp_substr needs column")?)?;
        let pattern = parts
            .get(1)
            .ok_or("regexp_substr needs pattern")?
            .trim_matches(['\'', '"']);
        let c = col(col_name);
        return Ok(regexp_substr(&c, pattern).into_expr());
    }

    // Handle split_part(col('name'), delimiter, part_num)
    if s.starts_with("split_part(") {
        let inner = extract_first_arg(s, "split_part(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("split_part needs column")?)?;
        let delimiter = parts
            .get(1)
            .ok_or("split_part needs delimiter")?
            .trim()
            .trim_matches(['\'', '"']);
        let part_num: i64 = parts
            .get(2)
            .ok_or("split_part needs part_num")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let c = col(col_name);
        return Ok(split_part(&c, delimiter, part_num).into_expr());
    }

    // Handle find_in_set(col('str'), col('set'))
    if s.starts_with("find_in_set(") {
        let inner = extract_first_arg(s, "find_in_set(")?;
        let parts = parse_comma_separated_args(inner);
        let str_col_name = extract_col_name(parts.first().ok_or("find_in_set needs str column")?)?;
        let set_col_name = extract_col_name(parts.get(1).ok_or("find_in_set needs set column")?)?;
        let str_c = col(str_col_name);
        let set_c = col(set_col_name);
        return Ok(find_in_set(&str_c, &set_c).into_expr());
    }

    // Handle format_string('%d %s', col('a'), col('b')) and printf(...)
    if s.starts_with("format_string(") || s.starts_with("printf(") {
        let prefix = if s.starts_with("format_string(") {
            "format_string("
        } else {
            "printf("
        };
        let inner = extract_first_arg(s, prefix)?;
        let parts = parse_comma_separated_args(inner);
        let format_str = parts
            .first()
            .ok_or("format_string/printf needs format")?
            .trim()
            .trim_matches(['\'', '"']);
        let mut cols: Vec<robin_sparkless::Column> = Vec::new();
        for p in parts.iter().skip(1) {
            let col_name = extract_col_name(p.trim())?;
            cols.push(col(col_name));
        }
        let col_refs: Vec<&robin_sparkless::Column> = cols.iter().collect();
        return Ok(format_string(format_str, &col_refs).into_expr());
    }

    // Handle unix_timestamp() - 0-arg
    if s == "unix_timestamp()" {
        return Ok(unix_timestamp_now().into_expr());
    }

    // Handle unix_timestamp(col('x')) or unix_timestamp(col('x'), 'yyyy-MM-dd')
    if s.starts_with("unix_timestamp(") {
        let inner = extract_first_arg(s, "unix_timestamp(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("unix_timestamp needs column")?)?;
        let format = parts.get(1).map(|p| p.trim().trim_matches(['\'', '"']));
        let c = col(col_name);
        return Ok(unix_timestamp(&c, format).into_expr());
    }

    // Handle to_unix_timestamp(col('x')) or to_unix_timestamp(col('x'), 'yyyy-MM-dd')
    if s.starts_with("to_unix_timestamp(") {
        let inner = extract_first_arg(s, "to_unix_timestamp(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("to_unix_timestamp needs column")?)?;
        let format = parts.get(1).map(|p| p.trim().trim_matches(['\'', '"']));
        let c = col(col_name);
        return Ok(to_unix_timestamp(&c, format).into_expr());
    }

    // Handle from_unixtime(col('x')) or from_unixtime(col('x'), 'yyyy-MM-dd')
    if s.starts_with("from_unixtime(") {
        let inner = extract_first_arg(s, "from_unixtime(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("from_unixtime needs column")?)?;
        let format = parts.get(1).map(|p| p.trim().trim_matches(['\'', '"']));
        let c = col(col_name);
        return Ok(from_unixtime(&c, format).into_expr());
    }

    // Handle make_date(col('y'), col('m'), col('d'))
    if s.starts_with("make_date(") {
        let inner = extract_first_arg(s, "make_date(")?;
        let parts = parse_comma_separated_args(inner);
        let y_name = extract_col_name(parts.first().ok_or("make_date needs year")?)?;
        let m_name = extract_col_name(parts.get(1).ok_or("make_date needs month")?)?;
        let d_name = extract_col_name(parts.get(2).ok_or("make_date needs day")?)?;
        let yc = col(y_name);
        let mc = col(m_name);
        let dc = col(d_name);
        return Ok(make_date(&yc, &mc, &dc).into_expr());
    }

    // Handle make_timestamp(col('y'), col('mo'), col('d'), col('h'), col('mi'), col('s')) or with optional timezone
    if s.starts_with("make_timestamp(") {
        let inner = extract_first_arg(s, "make_timestamp(")?;
        let parts = parse_comma_separated_args(inner);
        let y_name = extract_col_name(parts.first().ok_or("make_timestamp needs year")?)?;
        let mo_name = extract_col_name(parts.get(1).ok_or("make_timestamp needs month")?)?;
        let d_name = extract_col_name(parts.get(2).ok_or("make_timestamp needs day")?)?;
        let h_name = extract_col_name(parts.get(3).ok_or("make_timestamp needs hour")?)?;
        let mi_name = extract_col_name(parts.get(4).ok_or("make_timestamp needs minute")?)?;
        let s_name = extract_col_name(parts.get(5).ok_or("make_timestamp needs second")?)?;
        let timezone = parts.get(6).map(|p| p.trim().trim_matches(['\'', '"']));
        let yc = col(y_name);
        let moc = col(mo_name);
        let dc = col(d_name);
        let hc = col(h_name);
        let mic = col(mi_name);
        let sc = col(s_name);
        return Ok(make_timestamp(&yc, &moc, &dc, &hc, &mic, &sc, timezone).into_expr());
    }

    // Handle timestampadd('DAY', col('delta'), col('ts'))
    if s.starts_with("timestampadd(") {
        let inner = extract_first_arg(s, "timestampadd(")?;
        let parts = parse_comma_separated_args(inner);
        let unit = parts
            .first()
            .ok_or("timestampadd needs unit")?
            .trim()
            .trim_matches(['\'', '"']);
        let amount_name = extract_col_name(parts.get(1).ok_or("timestampadd needs amount")?)?;
        let ts_name = extract_col_name(parts.get(2).ok_or("timestampadd needs timestamp")?)?;
        return Ok(timestampadd(unit, &col(amount_name), &col(ts_name)).into_expr());
    }

    // Handle timestampdiff('DAY', col('start'), col('end'))
    if s.starts_with("timestampdiff(") {
        let inner = extract_first_arg(s, "timestampdiff(")?;
        let parts = parse_comma_separated_args(inner);
        let unit = parts
            .first()
            .ok_or("timestampdiff needs unit")?
            .trim()
            .trim_matches(['\'', '"']);
        let start_name = extract_col_name(parts.get(1).ok_or("timestampdiff needs start")?)?;
        let end_name = extract_col_name(parts.get(2).ok_or("timestampdiff needs end")?)?;
        return Ok(timestampdiff(unit, &col(start_name), &col(end_name)).into_expr());
    }

    // Handle from_utc_timestamp(col('ts'), 'America/Los_Angeles')
    if s.starts_with("from_utc_timestamp(") {
        let inner = extract_first_arg(s, "from_utc_timestamp(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("from_utc_timestamp needs column")?)?;
        let tz = parts
            .get(1)
            .ok_or("from_utc_timestamp needs timezone")?
            .trim()
            .trim_matches(['\'', '"']);
        return Ok(from_utc_timestamp(&col(col_name), tz).into_expr());
    }

    // Handle to_utc_timestamp(col('ts'), 'America/Los_Angeles')
    if s.starts_with("to_utc_timestamp(") {
        let inner = extract_first_arg(s, "to_utc_timestamp(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("to_utc_timestamp needs column")?)?;
        let tz = parts
            .get(1)
            .ok_or("to_utc_timestamp needs timezone")?
            .trim()
            .trim_matches(['\'', '"']);
        return Ok(to_utc_timestamp(&col(col_name), tz).into_expr());
    }

    // Handle url_decode(col('x')), url_encode(col('x'))
    if s.starts_with("url_decode(") {
        let inner = extract_first_arg(s, "url_decode(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(url_decode(&col(col_name)).into_expr());
    }
    if s.starts_with("url_encode(") {
        let inner = extract_first_arg(s, "url_encode(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(url_encode(&col(col_name)).into_expr());
    }

    // Handle json_array_length(col('json'), '$.arr')
    if s.starts_with("json_array_length(") {
        let inner = extract_first_arg(s, "json_array_length(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("json_array_length needs column")?)?;
        let path = parts
            .get(1)
            .ok_or("json_array_length needs path")?
            .trim()
            .trim_matches(['\'', '"']);
        return Ok(json_array_length(&col(col_name), path).into_expr());
    }

    // Handle parse_url(col('url'), 'HOST') or parse_url(col('url'), 'QUERY', 'k')
    if s.starts_with("parse_url(") {
        let inner = extract_first_arg(s, "parse_url(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("parse_url needs column")?)?;
        let part = parts
            .get(1)
            .ok_or("parse_url needs part")?
            .trim()
            .trim_matches(['\'', '"']);
        let key = parts
            .get(2)
            .map(|p| p.trim().trim_matches(['\'', '"']).to_string());
        return Ok(parse_url(&col(col_name), part, key.as_deref()).into_expr());
    }

    // Handle isin(col('x'), 1, 2, 3) or isin(col('x'), 'a', 'b', 'c')
    if s.starts_with("isin(") {
        let inner = extract_first_arg(s, "isin(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("isin needs column")?)?;
        let rest: Vec<&str> = parts.iter().skip(1).map(|p| p.trim()).collect();
        if rest.is_empty() {
            return Err("isin needs at least one value".to_string());
        }
        let first = rest[0].trim_matches(['\'', '"']);
        if first.parse::<i64>().is_ok() {
            let values: Vec<i64> = rest
                .iter()
                .filter_map(|p| p.trim().trim_matches(['\'', '"']).parse::<i64>().ok())
                .collect();
            return Ok(isin_i64(&col(col_name), &values).into_expr());
        }
        let values: Vec<&str> = rest
            .iter()
            .map(|p| p.trim().trim_matches(['\'', '"']))
            .collect();
        return Ok(isin_str(&col(col_name), &values).into_expr());
    }

    // Handle hash(col('a')) or hash(col('a'), col('b'))
    if s.starts_with("hash(") {
        let inner = extract_first_arg(s, "hash(")?;
        let parts = parse_comma_separated_args(inner);
        let cols: Vec<_> = parts
            .iter()
            .map(|p| extract_col_name(p.trim()))
            .collect::<Result<Vec<_>, _>>()?;
        if cols.is_empty() {
            return Err("hash needs at least one column".to_string());
        }
        let refs: Vec<_> = cols.iter().map(|c| col(c)).collect();
        let col_refs: Vec<_> = refs.iter().collect();
        return Ok(hash(&col_refs).into_expr());
    }

    // Handle shiftLeft(col('x'), 2) or shift_left
    if s.starts_with("shiftLeft(") || s.starts_with("shift_left(") {
        let prefix = if s.starts_with("shiftLeft(") {
            "shiftLeft("
        } else {
            "shift_left("
        };
        let inner = extract_first_arg(s, prefix)?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("shiftLeft needs column")?)?;
        let n: i32 = parts
            .get(1)
            .ok_or("shiftLeft needs n")?
            .trim()
            .parse()
            .map_err(|_| "shiftLeft n must be integer")?;
        return Ok(shift_left(&col(col_name), n).into_expr());
    }

    // Handle shiftRight(col('x'), 2)
    if s.starts_with("shiftRight(") || s.starts_with("shift_right(") {
        let prefix = if s.starts_with("shiftRight(") {
            "shiftRight("
        } else {
            "shift_right("
        };
        let inner = extract_first_arg(s, prefix)?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("shiftRight needs column")?)?;
        let n: i32 = parts
            .get(1)
            .ok_or("shiftRight needs n")?
            .trim()
            .parse()
            .map_err(|_| "shiftRight n must be integer")?;
        return Ok(shift_right(&col(col_name), n).into_expr());
    }

    // Handle version()
    if s == "version()" {
        return Ok(version().into_expr());
    }

    // Handle equal_null(col('a'), col('b'))
    if s.starts_with("equal_null(") {
        let inner = extract_first_arg(s, "equal_null(")?;
        let parts = parse_comma_separated_args(inner);
        let a_name = extract_col_name(parts.first().ok_or("equal_null needs a")?)?;
        let b_name = extract_col_name(parts.get(1).ok_or("equal_null needs b")?)?;
        return Ok(equal_null(&col(a_name), &col(b_name)).into_expr());
    }

    // Handle curdate(), now(), localtimestamp() - with optional mock for deterministic tests
    if s == "curdate()" || s == "current_date()" {
        if mock_dates {
            let d = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
            return Ok(lit(d).cast(DataType::Date));
        }
        return Ok(if s == "curdate()" {
            curdate()
        } else {
            current_date()
        }
        .into_expr());
    }
    if s == "now()" || s == "current_timestamp()" {
        if mock_dates {
            let dt =
                NaiveDateTime::parse_from_str("2025-01-15 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
            return Ok(lit(dt));
        }
        return Ok(if s == "now()" {
            now()
        } else {
            current_timestamp()
        }
        .into_expr());
    }
    if s == "localtimestamp()" {
        return Ok(localtimestamp().into_expr());
    }

    // Handle extract(col('x'), 'year') or extract('year', col('x'))
    if s.starts_with("extract(") {
        let inner = extract_first_arg(s, "extract(")?;
        let parts = parse_comma_separated_args(inner);
        let (col_name, field) = if parts[0].contains("col(") {
            let col_name = extract_col_name(parts.first().ok_or("extract needs column")?)?;
            let field = parts
                .get(1)
                .ok_or("extract needs field")?
                .trim()
                .trim_matches(['\'', '"']);
            (col_name, field)
        } else {
            let field = parts
                .first()
                .ok_or("extract needs field")?
                .trim()
                .trim_matches(['\'', '"']);
            let col_name = extract_col_name(parts.get(1).ok_or("extract needs column")?)?;
            (col_name, field)
        };
        return Ok(extract(&col(col_name), field).into_expr());
    }

    // Handle dayname(col('x')), weekday(col('x'))
    if s.starts_with("dayname(") {
        let inner = extract_first_arg(s, "dayname(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(dayname(&col(col_name)).into_expr());
    }
    if s.starts_with("weekday(") {
        let inner = extract_first_arg(s, "weekday(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(weekday(&col(col_name)).into_expr());
    }

    // Handle unix_micros(col), unix_millis(col), unix_seconds(col)
    if s.starts_with("unix_micros(") {
        let inner = extract_first_arg(s, "unix_micros(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(unix_micros(&col(col_name)).into_expr());
    }
    if s.starts_with("unix_millis(") {
        let inner = extract_first_arg(s, "unix_millis(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(unix_millis(&col(col_name)).into_expr());
    }
    if s.starts_with("unix_seconds(") {
        let inner = extract_first_arg(s, "unix_seconds(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(unix_seconds(&col(col_name)).into_expr());
    }

    // Handle timestamp_seconds(col('x')), timestamp_millis(col('x')), timestamp_micros(col('x'))
    if s.starts_with("timestamp_seconds(") {
        let inner = extract_first_arg(s, "timestamp_seconds(")?;
        let col_name = extract_col_name(inner.trim())?;
        return Ok(timestamp_seconds(&col(col_name)).into_expr());
    }
    if s.starts_with("timestamp_millis(") {
        let inner = extract_first_arg(s, "timestamp_millis(")?;
        let col_name = extract_col_name(inner.trim())?;
        return Ok(timestamp_millis(&col(col_name)).into_expr());
    }
    if s.starts_with("timestamp_micros(") {
        let inner = extract_first_arg(s, "timestamp_micros(")?;
        let col_name = extract_col_name(inner.trim())?;
        return Ok(timestamp_micros(&col(col_name)).into_expr());
    }

    // Handle unix_date(col('d'))
    if s.starts_with("unix_date(") {
        let inner = extract_first_arg(s, "unix_date(")?;
        let col_name = extract_col_name(inner.trim())?;
        return Ok(unix_date(&col(col_name)).into_expr());
    }

    // Handle date_from_unix_date(col('d'))
    if s.starts_with("date_from_unix_date(") {
        let inner = extract_first_arg(s, "date_from_unix_date(")?;
        let col_name = extract_col_name(inner.trim())?;
        return Ok(date_from_unix_date(&col(col_name)).into_expr());
    }

    // Handle pmod(col('a'), col('b'))
    if s.starts_with("pmod(") {
        let inner = extract_first_arg(s, "pmod(")?;
        let parts = parse_comma_separated_args(inner);
        let a_name = extract_col_name(parts.first().ok_or("pmod needs dividend")?)?;
        let b_name = extract_col_name(parts.get(1).ok_or("pmod needs divisor")?)?;
        return Ok(pmod(&col(a_name), &col(b_name)).into_expr());
    }

    // Handle factorial(col('n'))
    if s.starts_with("factorial(") {
        let inner = extract_first_arg(s, "factorial(")?;
        let col_name = extract_col_name(inner.trim())?;
        return Ok(factorial(&col(col_name)).into_expr());
    }

    // Handle split(col('name'), delimiter)
    if s.starts_with("split(") {
        let inner = extract_first_arg(s, "split(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("split needs column")?)?;
        let delimiter = parts
            .get(1)
            .ok_or("split needs delimiter")?
            .trim()
            .trim_matches(['\'', '"'])
            .trim();
        let c = col(col_name);
        return Ok(split(&c, delimiter).into_expr());
    }

    // Handle left(col('name'), n), right(col('name'), n)
    if s.starts_with("left(") {
        let inner = extract_first_arg(s, "left(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("left needs column")?)?;
        let n: i64 = parts
            .get(1)
            .ok_or("left needs n")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let c = col(col_name);
        return Ok(left(&c, n).into_expr());
    }
    if s.starts_with("right(") {
        let inner = extract_first_arg(s, "right(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("right needs column")?)?;
        let n: i64 = parts
            .get(1)
            .ok_or("right needs n")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let c = col(col_name);
        return Ok(right(&c, n).into_expr());
    }

    // Handle replace(col('name'), search, replacement)
    if s.starts_with("replace(") {
        let inner = extract_first_arg(s, "replace(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("replace needs column")?)?;
        let search = parts
            .get(1)
            .ok_or("replace needs search")?
            .trim()
            .trim_matches(['\'', '"']);
        let replacement = parts
            .get(2)
            .ok_or("replace needs replacement")?
            .trim()
            .trim_matches(['\'', '"']);
        let c = col(col_name);
        return Ok(replace(&c, search, replacement).into_expr());
    }

    // Handle startswith(col('name'), prefix), endswith(col('name'), suffix), contains(col('name'), substr)
    if s.starts_with("startswith(") {
        let inner = extract_first_arg(s, "startswith(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("startswith needs column")?)?;
        let prefix = parts
            .get(1)
            .ok_or("startswith needs prefix")?
            .trim()
            .trim_matches(['\'', '"']);
        let c = col(col_name);
        return Ok(startswith(&c, prefix).into_expr());
    }
    if s.starts_with("endswith(") {
        let inner = extract_first_arg(s, "endswith(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("endswith needs column")?)?;
        let suffix = parts
            .get(1)
            .ok_or("endswith needs suffix")?
            .trim()
            .trim_matches(['\'', '"']);
        let c = col(col_name);
        return Ok(endswith(&c, suffix).into_expr());
    }
    if s.starts_with("contains(") {
        let inner = extract_first_arg(s, "contains(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("contains needs column")?)?;
        let substring = parts
            .get(1)
            .ok_or("contains needs substring")?
            .trim()
            .trim_matches(['\'', '"']);
        let c = col(col_name);
        return Ok(contains(&c, substring).into_expr());
    }

    // Handle like(col('name'), pattern), like(..., escapeChar), ilike(...), rlike(...)
    if s.starts_with("like(") {
        let inner = extract_first_arg(s, "like(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("like needs column")?)?;
        let pattern = parts
            .get(1)
            .ok_or("like needs pattern")?
            .trim()
            .trim_matches(['\'', '"']);
        let escape_char = parts
            .get(2)
            .and_then(|p| p.trim_matches(['\'', '"']).chars().next());
        let c = col(col_name);
        return Ok(like(&c, pattern, escape_char).into_expr());
    }
    if s.starts_with("ilike(") {
        let inner = extract_first_arg(s, "ilike(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("ilike needs column")?)?;
        let pattern = parts
            .get(1)
            .ok_or("ilike needs pattern")?
            .trim()
            .trim_matches(['\'', '"']);
        let escape_char = parts
            .get(2)
            .and_then(|p| p.trim_matches(['\'', '"']).chars().next());
        let c = col(col_name);
        return Ok(ilike(&c, pattern, escape_char).into_expr());
    }
    if s.starts_with("rlike(") || s.starts_with("regexp(") {
        let prefix = if s.starts_with("rlike(") {
            "rlike("
        } else {
            "regexp("
        };
        let inner = extract_first_arg(s, prefix)?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("rlike needs column")?)?;
        let pattern = parts
            .get(1)
            .ok_or("rlike needs pattern")?
            .trim()
            .trim_matches(['\'', '"']);
        let c = col(col_name);
        return Ok(rlike(&c, pattern).into_expr());
    }

    // Handle array_contains(col('arr'), lit('x')) or array_contains(col('arr'), lit(1))
    if s.starts_with("array_contains(") {
        let inner = extract_first_arg(s, "array_contains(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("array_contains needs column")?)?;
        let arr_col = col(col_name);
        let value_expr =
            parse_column_or_literal(parts.get(1).ok_or("array_contains needs value")?.trim())?;
        let value_col = robin_sparkless::Column::from_expr(value_expr, None);
        return Ok(array_contains(&arr_col, &value_col).into_expr());
    }
    if s.starts_with("array_append(") {
        let inner = extract_first_arg(s, "array_append(")?;
        let parts = parse_comma_separated_args(inner);
        let arr_name = extract_col_name(parts.first().ok_or("array_append needs array")?)?;
        let elem_expr =
            parse_column_or_literal(parts.get(1).ok_or("array_append needs element")?.trim())?;
        let arr_col = col(arr_name);
        let elem_col = robin_sparkless::Column::from_expr(elem_expr, None);
        return Ok(array_append(&arr_col, &elem_col).into_expr());
    }
    if s.starts_with("array_prepend(") {
        let inner = extract_first_arg(s, "array_prepend(")?;
        let parts = parse_comma_separated_args(inner);
        let arr_name = extract_col_name(parts.first().ok_or("array_prepend needs array")?)?;
        let elem_expr =
            parse_column_or_literal(parts.get(1).ok_or("array_prepend needs element")?.trim())?;
        let arr_col = col(arr_name);
        let elem_col = robin_sparkless::Column::from_expr(elem_expr, None);
        return Ok(array_prepend(&arr_col, &elem_col).into_expr());
    }
    if s.starts_with("array_insert(") {
        let inner = extract_first_arg(s, "array_insert(")?;
        let parts = parse_comma_separated_args(inner);
        let arr_name = extract_col_name(parts.first().ok_or("array_insert needs array")?)?;
        let pos_expr =
            parse_column_or_literal(parts.get(1).ok_or("array_insert needs position")?.trim())?;
        let elem_expr =
            parse_column_or_literal(parts.get(2).ok_or("array_insert needs element")?.trim())?;
        let arr_col = col(arr_name);
        let pos_col = robin_sparkless::Column::from_expr(pos_expr, None);
        let elem_col = robin_sparkless::Column::from_expr(elem_expr, None);
        return Ok(array_insert(&arr_col, &pos_col, &elem_col).into_expr());
    }
    if s.starts_with("array_except(") {
        let inner = extract_first_arg(s, "array_except(")?;
        let parts = parse_comma_separated_args(inner);
        let a_name = extract_col_name(parts.first().ok_or("array_except needs first array")?)?;
        let b_name = extract_col_name(parts.get(1).ok_or("array_except needs second array")?)?;
        return Ok(array_except(&col(a_name), &col(b_name)).into_expr());
    }
    if s.starts_with("array_intersect(") {
        let inner = extract_first_arg(s, "array_intersect(")?;
        let parts = parse_comma_separated_args(inner);
        let a_name = extract_col_name(parts.first().ok_or("array_intersect needs first array")?)?;
        let b_name = extract_col_name(parts.get(1).ok_or("array_intersect needs second array")?)?;
        return Ok(array_intersect(&col(a_name), &col(b_name)).into_expr());
    }
    if s.starts_with("array_union(") {
        let inner = extract_first_arg(s, "array_union(")?;
        let parts = parse_comma_separated_args(inner);
        let a_name = extract_col_name(parts.first().ok_or("array_union needs first array")?)?;
        let b_name = extract_col_name(parts.get(1).ok_or("array_union needs second array")?)?;
        return Ok(array_union(&col(a_name), &col(b_name)).into_expr());
    }
    if s.starts_with("create_map(") {
        let inner = extract_first_arg(s, "create_map(")?;
        let parts = parse_comma_separated_args(inner);
        if parts.len() < 2 || !parts.len().is_multiple_of(2) {
            return Err("create_map needs key-value pairs".to_string());
        }
        let mut cols: Vec<robin_sparkless::Column> = Vec::new();
        for p in &parts {
            let expr = parse_column_or_literal(p.trim())?;
            cols.push(robin_sparkless::Column::from_expr(expr, None));
        }
        let col_refs: Vec<&robin_sparkless::Column> = cols.iter().collect();
        return Ok(create_map(&col_refs)
            .map_err(|e| e.to_string())?
            .into_expr());
    }
    if s.starts_with("map_concat(") {
        let inner = extract_first_arg(s, "map_concat(")?;
        let parts = parse_comma_separated_args(inner);
        let a_name = extract_col_name(parts.first().ok_or("map_concat needs first map")?)?;
        let b_name = extract_col_name(parts.get(1).ok_or("map_concat needs second map")?)?;
        return Ok(map_concat(&col(a_name), &col(b_name)).into_expr());
    }
    if s.starts_with("map_from_entries(") {
        let inner = extract_first_arg(s, "map_from_entries(")?;
        let col_name = extract_col_name(inner.trim())?;
        return Ok(map_from_entries(&col(col_name)).into_expr());
    }
    if s.starts_with("map_contains_key(") {
        let inner = extract_first_arg(s, "map_contains_key(")?;
        let parts = parse_comma_separated_args(inner);
        let map_name = extract_col_name(parts.first().ok_or("map_contains_key needs map")?)?;
        let key_expr =
            parse_column_or_literal(parts.get(1).ok_or("map_contains_key needs key")?.trim())?;
        let key_col = robin_sparkless::Column::from_expr(key_expr, None);
        return Ok(map_contains_key(&col(map_name), &key_col).into_expr());
    }
    if s.starts_with("get(") && !s.starts_with("get_json_object(") {
        let inner = extract_first_arg(s, "get(")?;
        let parts = parse_comma_separated_args(inner);
        let map_name = extract_col_name(parts.first().ok_or("get needs map")?)?;
        let key_expr = parse_column_or_literal(parts.get(1).ok_or("get needs key")?.trim())?;
        let key_col = robin_sparkless::Column::from_expr(key_expr, None);
        return Ok(get(&col(map_name), &key_col).into_expr());
    }
    if s.starts_with("map_filter(") {
        let inner = extract_first_arg(s, "map_filter(")?;
        let parts = parse_comma_separated_args(inner);
        let map_name = extract_col_name(parts.first().ok_or("map_filter needs map")?)?;
        let pred_str = parts.get(1).ok_or("map_filter needs predicate")?.trim();
        let pred = parse_map_filter_predicate(pred_str)?;
        return Ok(map_filter(&col(map_name), pred).into_expr());
    }
    if s.starts_with("zip_with(") {
        let inner = extract_first_arg(s, "zip_with(")?;
        let parts = parse_comma_separated_args(inner);
        let a_name = extract_col_name(parts.first().ok_or("zip_with needs first array")?)?;
        let b_name = extract_col_name(parts.get(1).ok_or("zip_with needs second array")?)?;
        let merge_str = parts.get(2).ok_or("zip_with needs merge expr")?.trim();
        let merge = parse_zip_with_merge(merge_str)?;
        return Ok(zip_with(&col(a_name), &col(b_name), merge).into_expr());
    }
    if s.starts_with("map_zip_with(") {
        let inner = extract_first_arg(s, "map_zip_with(")?;
        let parts = parse_comma_separated_args(inner);
        let m1_name = extract_col_name(parts.first().ok_or("map_zip_with needs first map")?)?;
        let m2_name = extract_col_name(parts.get(1).ok_or("map_zip_with needs second map")?)?;
        let merge_str = parts.get(2).ok_or("map_zip_with needs merge expr")?.trim();
        let merge = parse_map_zip_with_merge(merge_str)?;
        return Ok(map_zip_with(&col(m1_name), &col(m2_name), merge).into_expr());
    }
    if s.starts_with("try_divide(") {
        let inner = extract_first_arg(s, "try_divide(")?;
        let parts = parse_comma_separated_args(inner);
        let a_name = extract_col_name(parts.first().ok_or("try_divide needs left")?)?;
        let b_name = extract_col_name(parts.get(1).ok_or("try_divide needs right")?)?;
        return Ok(try_divide(&col(a_name), &col(b_name)).into_expr());
    }
    if s.starts_with("try_add(") {
        let inner = extract_first_arg(s, "try_add(")?;
        let parts = parse_comma_separated_args(inner);
        let a_name = extract_col_name(parts.first().ok_or("try_add needs left")?)?;
        let b_name = extract_col_name(parts.get(1).ok_or("try_add needs right")?)?;
        return Ok(try_add(&col(a_name), &col(b_name)).into_expr());
    }
    if s.starts_with("try_subtract(") {
        let inner = extract_first_arg(s, "try_subtract(")?;
        let parts = parse_comma_separated_args(inner);
        let a_name = extract_col_name(parts.first().ok_or("try_subtract needs left")?)?;
        let b_name = extract_col_name(parts.get(1).ok_or("try_subtract needs right")?)?;
        return Ok(try_subtract(&col(a_name), &col(b_name)).into_expr());
    }
    if s.starts_with("try_multiply(") {
        let inner = extract_first_arg(s, "try_multiply(")?;
        let parts = parse_comma_separated_args(inner);
        let a_name = extract_col_name(parts.first().ok_or("try_multiply needs left")?)?;
        let b_name = extract_col_name(parts.get(1).ok_or("try_multiply needs right")?)?;
        return Ok(try_multiply(&col(a_name), &col(b_name)).into_expr());
    }
    if s.starts_with("width_bucket(") {
        let inner = extract_first_arg(s, "width_bucket(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("width_bucket needs value")?)?;
        let min_val: f64 = parts
            .get(1)
            .ok_or("width_bucket needs min")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseFloatError| e.to_string())?;
        let max_val: f64 = parts
            .get(2)
            .ok_or("width_bucket needs max")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseFloatError| e.to_string())?;
        let nb: i64 = parts
            .get(3)
            .ok_or("width_bucket needs num_bucket")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        return Ok(width_bucket(&col(col_name), min_val, max_val, nb).into_expr());
    }
    if s.starts_with("elt(") {
        let inner = extract_first_arg(s, "elt(")?;
        let parts = parse_comma_separated_args(inner);
        if parts.len() < 2 {
            return Err("elt needs index and at least one column".to_string());
        }
        let idx_name = extract_col_name(parts.first().ok_or("elt needs index")?)?;
        let mut cols: Vec<robin_sparkless::Column> = Vec::new();
        for p in parts.iter().skip(1) {
            let name = extract_col_name(p.trim())?;
            cols.push(col(name));
        }
        let col_refs: Vec<&robin_sparkless::Column> = cols.iter().collect();
        return Ok(elt(&col(idx_name), &col_refs).into_expr());
    }
    if s.starts_with("bit_length(") {
        let inner = extract_first_arg(s, "bit_length(")?;
        let col_name = extract_col_name(inner.trim())?;
        return Ok(bit_length(&col(col_name)).into_expr());
    }
    if s.starts_with("octet_length(") {
        let inner = extract_first_arg(s, "octet_length(")?;
        let col_name = extract_col_name(inner.trim())?;
        return Ok(octet_length(&col(col_name)).into_expr());
    }
    if s.starts_with("typeof(") {
        let inner = extract_first_arg(s, "typeof(")?;
        let col_name = extract_col_name(inner.trim())?;
        return Ok(typeof_(&col(col_name)).into_expr());
    }
    if s.starts_with("struct(") {
        let inner = extract_first_arg(s, "struct(")?;
        let parts = parse_comma_separated_args(inner);
        let mut cols: Vec<robin_sparkless::Column> = Vec::new();
        for p in &parts {
            let name = extract_col_name(p.trim())?;
            cols.push(col(name));
        }
        let col_refs: Vec<&robin_sparkless::Column> = cols.iter().collect();
        return Ok(struct_(&col_refs).into_expr());
    }
    if s.starts_with("named_struct(") {
        let inner = extract_first_arg(s, "named_struct(")?;
        let parts = parse_comma_separated_args(inner);
        if parts.len() < 2 || !parts.len().is_multiple_of(2) {
            return Err("named_struct needs (name, column) pairs".to_string());
        }
        let mut pairs: Vec<(&str, robin_sparkless::Column)> = Vec::new();
        for i in (0..parts.len()).step_by(2) {
            let name_str = parts
                .get(i)
                .ok_or("named_struct: missing name")?
                .trim()
                .trim_matches(['\'', '"']);
            let col_name = extract_col_name(
                parts
                    .get(i + 1)
                    .ok_or("named_struct: missing column")?
                    .trim(),
            )?;
            pairs.push((name_str, col(col_name)));
        }
        let pair_refs: Vec<(&str, &robin_sparkless::Column)> =
            pairs.iter().map(|(n, c)| (*n, c)).collect();
        return Ok(named_struct(pair_refs.as_slice()).into_expr());
    }

    // Handle element_at(col('arr'), 1) - 1-based index
    if s.starts_with("element_at(") {
        let inner = extract_first_arg(s, "element_at(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("element_at needs column")?)?;
        let index: i64 = parts
            .get(1)
            .ok_or("element_at needs index")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let c = col(col_name);
        return Ok(element_at(&c, index).into_expr());
    }

    // Handle size(col('arr')) or array_size(col('arr'))
    if s.starts_with("size(") {
        let inner = extract_first_arg(s, "size(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(size(&c).into_expr());
    }
    if s.starts_with("array_size(") {
        let inner = extract_first_arg(s, "array_size(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(array_size(&c).into_expr());
    }
    if s.starts_with("array_sum(") {
        let inner = extract_first_arg(s, "array_sum(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(array_sum(&c).into_expr());
    }
    if s.starts_with("array(") {
        let inner = extract_first_arg(s, "array(")?;
        let parts = parse_comma_separated_args(inner);
        let mut exprs: Vec<Expr> = Vec::with_capacity(parts.len());
        for p in &parts {
            let name = extract_col_name(p)?;
            exprs.push(col(name).expr().clone());
        }
        return Ok(concat_list(exprs).expect("concat_list"));
    }

    // Handle substring(col('name'), start, length) - 1-based start
    if s.starts_with("substring(") {
        let inner = extract_first_arg(s, "substring(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("substring needs column")?)?;
        let start: i64 = parts
            .get(1)
            .ok_or("substring needs start")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let length: Option<i64> = parts.get(2).and_then(|a| a.trim().parse().ok());
        let c = col(col_name);
        return Ok(substring(&c, start, length).into_expr());
    }

    // Handle concat(col('a'), col('b'), lit(' '), ...)
    if s.starts_with("concat(") {
        let inner = &s[7..s.len() - 1];
        let parts = parse_comma_separated_args(inner);
        let mut columns: Vec<robin_sparkless::Column> = Vec::new();
        for part in parts {
            columns.push(parse_column_or_literal_for_concat(part)?);
        }
        if columns.is_empty() {
            return Err("concat requires at least one argument".to_string());
        }
        let col_refs: Vec<&robin_sparkless::Column> = columns.iter().collect();
        return Ok(concat(&col_refs).into_expr());
    }

    // Handle concat_ws('-', col('a'), col('b'), ...)
    if s.starts_with("concat_ws(") {
        let inner = &s[10..s.len() - 1];
        let parts = parse_comma_separated_args(inner);
        let separator = parts
            .first()
            .ok_or("concat_ws needs separator")?
            .trim_matches(['\'', '"']);
        let mut columns: Vec<robin_sparkless::Column> = Vec::new();
        for part in parts.iter().skip(1) {
            columns.push(parse_column_or_literal_for_concat(part)?);
        }
        if columns.is_empty() {
            return Err("concat_ws requires at least one column".to_string());
        }
        let col_refs: Vec<&robin_sparkless::Column> = columns.iter().collect();
        return Ok(concat_ws(separator, &col_refs).into_expr());
    }

    // Handle standalone lit(None) - create a null column
    if s.trim() == "lit(None)" {
        use polars::prelude::*;
        // Create an expression that is always a null Int64 value
        return Ok(lit(NULL).cast(DataType::Int64));
    }

    // Handle col('x').between(lit(low), lit(high))
    if s.contains(".between(") {
        let between_pos = s.find(".between(").ok_or("missing .between(")?;
        let col_part = s[..between_pos].trim();
        let col_name = extract_col_name(col_part)?.to_string();
        let args_start = between_pos + 9; // len(".between(")
        let args_inner = &s[args_start..s.len() - 1]; // strip final ")"
        let parts = parse_comma_separated_args(args_inner);
        let low_str = parts.first().ok_or("between needs lower bound")?.trim();
        let high_str = parts.get(1).ok_or("between needs upper bound")?.trim();
        let low_expr = parse_column_or_literal(low_str)?;
        let high_expr = parse_column_or_literal(high_str)?;
        let c = polars::prelude::col(&col_name);
        return Ok(c.clone().gt_eq(low_expr).and(c.lt_eq(high_expr)));
    }

    // Handle coalesce() expressions
    if s.starts_with("coalesce(") {
        // Parse: coalesce(col('col1'), col('col2'), lit('default'))
        let inner = &s[9..s.len() - 1]; // Skip "coalesce(" and final ")"

        // Split by comma, but be careful with nested parentheses
        let mut parts: Vec<&str> = Vec::new();
        let mut start = 0;
        let mut depth = 0;
        for (i, ch) in inner.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => depth -= 1,
                ',' if depth == 0 => {
                    parts.push(inner[start..i].trim());
                    start = i + 1;
                }
                _ => {}
            }
        }
        parts.push(inner[start..].trim());

        let mut columns: Vec<robin_sparkless::Column> = Vec::new();
        for part in parts {
            let part = part.trim();
            if part.starts_with("col(") {
                // Extract column name: col('name') or col("name")
                let content = &part[4..part.len() - 1]; // Skip "col(" and ")"
                let col_name = content.trim_matches(['\'', '"']);
                columns.push(col(col_name));
            } else if part.starts_with("lit(") {
                // Extract literal value: lit('value') or lit("value")
                let content = &part[4..part.len() - 1]; // Skip "lit(" and ")"
                let lit_val = content.trim_matches(['\'', '"']);
                columns.push(lit_str(lit_val));
            } else if part.starts_with("when(") {
                // Recursively parse when() expression
                let expr = parse_with_column_expr(part, mock_dates)?;
                columns.push(robin_sparkless::Column::from_expr(expr, None));
            } else {
                // Try as a bare literal (quoted string or number)
                if (part.starts_with('\'') && part.ends_with('\''))
                    || (part.starts_with('"') && part.ends_with('"'))
                {
                    let lit_val = part.trim_matches(['\'', '"']);
                    columns.push(lit_str(lit_val));
                } else if let Ok(num) = part.parse::<i64>() {
                    // Numeric literal
                    use robin_sparkless::lit_i64;
                    columns.push(lit_i64(num));
                } else {
                    return Err(format!("unexpected part in coalesce: {part}"));
                }
            }
        }

        if columns.is_empty() {
            return Err("coalesce requires at least one argument".to_string());
        }

        let col_refs: Vec<&robin_sparkless::Column> = columns.iter().collect();
        let coalesce_col = coalesce(&col_refs);
        return Ok(coalesce_col.into_expr());
    }

    // --- String: repeat, reverse, instr, locate, lpad, rpad ---
    if s.starts_with("repeat(") {
        let inner = extract_first_arg(s, "repeat(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("repeat needs column")?)?;
        let n: i32 = parts
            .get(1)
            .ok_or("repeat needs n")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let c = col(col_name);
        return Ok(repeat(&c, n).into_expr());
    }
    if s.starts_with("btrim(") {
        let inner = extract_first_arg(s, "btrim(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("btrim needs column")?)?;
        let trim_str = parts.get(1).map(|p| p.trim_matches(['\'', '"']).trim());
        let c = col(col_name);
        return Ok(btrim(&c, trim_str).into_expr());
    }
    if s.starts_with("conv(") {
        let inner = extract_first_arg(s, "conv(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("conv needs column")?)?;
        let from_base: i32 = parts
            .get(1)
            .ok_or("conv needs from_base")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let to_base: i32 = parts
            .get(2)
            .ok_or("conv needs to_base")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let c = col(col_name);
        return Ok(conv(&c, from_base, to_base).into_expr());
    }
    if s.starts_with("hex(") {
        let inner = extract_first_arg(s, "hex(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(hex(&c).into_expr());
    }
    if s.starts_with("unhex(") {
        let inner = extract_first_arg(s, "unhex(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(unhex(&c).into_expr());
    }
    if s.starts_with("bin(") {
        let inner = extract_first_arg(s, "bin(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(bin(&c).into_expr());
    }
    if s.starts_with("getbit(") {
        let inner = extract_first_arg(s, "getbit(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("getbit needs column")?)?;
        let pos: i64 = parts
            .get(1)
            .ok_or("getbit needs pos")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let c = col(col_name);
        return Ok(getbit(&c, pos).into_expr());
    }
    if s.starts_with("to_char(") || s.starts_with("to_varchar(") {
        let func = if s.starts_with("to_char(") {
            "to_char("
        } else {
            "to_varchar("
        };
        let inner = extract_first_arg(s, func)?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("to_char needs column")?)?;
        let format = parts.get(1).map(|p| p.trim_matches(['\'', '"']));
        let c = col(col_name);
        return Ok(to_char(&c, format).into_expr());
    }
    if s.starts_with("to_number(") {
        let inner = extract_first_arg(s, "to_number(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("to_number needs column")?)?;
        let format = parts.get(1).map(|p| p.trim_matches(['\'', '"']));
        let c = col(col_name);
        return Ok(to_number(&c, format).into_expr());
    }
    if s.starts_with("try_to_number(") {
        let inner = extract_first_arg(s, "try_to_number(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("try_to_number needs column")?)?;
        let format = parts.get(1).map(|p| p.trim_matches(['\'', '"']));
        let c = col(col_name);
        return Ok(try_to_number(&c, format).into_expr());
    }
    if s.starts_with("try_to_timestamp(") {
        let inner = extract_first_arg(s, "try_to_timestamp(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("try_to_timestamp needs column")?)?;
        let format = parts.get(1).map(|p| p.trim_matches(['\'', '"']));
        let c = col(col_name);
        return Ok(try_to_timestamp(&c, format).into_expr());
    }
    if s.starts_with("to_timestamp(") {
        let inner = extract_first_arg(s, "to_timestamp(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("to_timestamp needs column")?)?;
        let format = parts.get(1).map(|p| p.trim_matches(['\'', '"']));
        let c = col(col_name);
        return Ok(to_timestamp(&c, format)?.into_expr());
    }
    if s.starts_with("str_to_map(") {
        let inner = extract_first_arg(s, "str_to_map(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("str_to_map needs column")?)?;
        let pair_delim = parts.get(1).map(|p| p.trim_matches(['\'', '"']));
        let key_value_delim = parts.get(2).map(|p| p.trim_matches(['\'', '"']));
        let c = col(col_name);
        return Ok(str_to_map(&c, pair_delim, key_value_delim).into_expr());
    }
    if s.starts_with("arrays_overlap(") {
        let inner = extract_first_arg(s, "arrays_overlap(")?;
        let parts = parse_comma_separated_args(inner);
        let col_a = extract_col_name(parts.first().ok_or("arrays_overlap needs col1")?)?;
        let col_b = extract_col_name(parts.get(1).ok_or("arrays_overlap needs col2")?)?;
        let a = col(col_a);
        let b = col(col_b);
        return Ok(arrays_overlap(&a, &b).into_expr());
    }
    if s.starts_with("arrays_zip(") {
        let inner = extract_first_arg(s, "arrays_zip(")?;
        let parts = parse_comma_separated_args(inner);
        let col_a = extract_col_name(parts.first().ok_or("arrays_zip needs col1")?)?;
        let col_b = extract_col_name(parts.get(1).ok_or("arrays_zip needs col2")?)?;
        let a = col(col_a);
        let b = col(col_b);
        return Ok(arrays_zip(&a, &b).into_expr());
    }
    if s.starts_with("reverse(") {
        let inner = extract_first_arg(s, "reverse(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(reverse(&c).into_expr());
    }
    if s.starts_with("instr(") {
        let inner = extract_first_arg(s, "instr(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("instr needs column")?)?;
        let substr = parts
            .get(1)
            .ok_or("instr needs substr")?
            .trim_matches(['\'', '"']);
        let c = col(col_name);
        return Ok(instr(&c, substr).into_expr());
    }
    if s.starts_with("locate(") {
        let inner = extract_first_arg(s, "locate(")?;
        let parts = parse_comma_separated_args(inner);
        let substr = parts
            .first()
            .ok_or("locate needs substr")?
            .trim_matches(['\'', '"']);
        let col_name = extract_col_name(parts.get(1).ok_or("locate needs column")?)?;
        let pos: i64 = parts
            .get(2)
            .map(|p| p.trim().parse().unwrap_or(1))
            .unwrap_or(1);
        let c = col(col_name);
        return Ok(locate(substr, &c, pos).into_expr());
    }
    if s.starts_with("lpad(") {
        let inner = extract_first_arg(s, "lpad(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("lpad needs column")?)?;
        let len: i32 = parts
            .get(1)
            .ok_or("lpad needs length")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let pad = parts
            .get(2)
            .ok_or("lpad needs pad")?
            .trim_matches(['\'', '"']);
        let c = col(col_name);
        return Ok(lpad(&c, len, pad).into_expr());
    }
    if s.starts_with("rpad(") {
        let inner = extract_first_arg(s, "rpad(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("rpad needs column")?)?;
        let len: i32 = parts
            .get(1)
            .ok_or("rpad needs length")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let pad = parts
            .get(2)
            .ok_or("rpad needs pad")?
            .trim_matches(['\'', '"']);
        let c = col(col_name);
        return Ok(rpad(&c, len, pad).into_expr());
    }
    if s.starts_with("ascii(") {
        let inner = extract_first_arg(s, "ascii(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(ascii(&c).into_expr());
    }
    if s.starts_with("format_number(") {
        let inner = extract_first_arg(s, "format_number(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("format_number needs column")?)?;
        let decimals: u32 = parts
            .get(1)
            .ok_or("format_number needs decimals")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let c = col(col_name);
        return Ok(format_number(&c, decimals).into_expr());
    }
    if s.starts_with("overlay(") {
        let inner = extract_first_arg(s, "overlay(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("overlay needs column")?)?;
        let replace = parts
            .get(1)
            .ok_or("overlay needs replace")?
            .trim_matches(['\'', '"']);
        let pos: i64 = parts
            .get(2)
            .ok_or("overlay needs pos")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let length: i64 = parts
            .get(3)
            .ok_or("overlay needs length")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let c = col(col_name);
        return Ok(overlay(&c, replace, pos, length).into_expr());
    }
    if s.starts_with("position(") {
        let inner = extract_first_arg(s, "position(")?;
        let parts = parse_comma_separated_args(inner);
        let substr = parts
            .first()
            .ok_or("position needs substr")?
            .trim_matches(['\'', '"']);
        let col_name = extract_col_name(parts.get(1).ok_or("position needs column")?)?;
        let pos: i64 = parts
            .get(2)
            .map(|p| p.trim().parse().unwrap_or(1))
            .unwrap_or(1);
        let c = col(col_name);
        return Ok(locate(substr, &c, pos).into_expr());
    }
    if s.starts_with("char(") {
        let inner = extract_first_arg(s, "char(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(rs_char(&c).into_expr());
    }
    if s.starts_with("chr(") {
        let inner = extract_first_arg(s, "chr(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(chr(&c).into_expr());
    }
    if s.starts_with("base64(") {
        let inner = extract_first_arg(s, "base64(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(base64(&c).into_expr());
    }
    if s.starts_with("unbase64(") {
        let inner = extract_first_arg(s, "unbase64(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(unbase64(&c).into_expr());
    }
    if s.starts_with("sha1(") {
        let inner = extract_first_arg(s, "sha1(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(sha1(&c).into_expr());
    }
    if s.starts_with("sha2(") {
        let inner = extract_first_arg(s, "sha2(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("sha2 needs column")?)?;
        let bit_length: i32 = parts
            .get(1)
            .ok_or("sha2 needs bit_length")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let c = col(col_name);
        return Ok(sha2(&c, bit_length).into_expr());
    }
    if s.starts_with("md5(") {
        let inner = extract_first_arg(s, "md5(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(md5(&c).into_expr());
    }
    if s.starts_with("array_compact(") {
        let inner = extract_first_arg(s, "array_compact(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(array_compact(&c).into_expr());
    }
    if s.starts_with("array_distinct(") {
        let inner = extract_first_arg(s, "array_distinct(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(array_distinct(&c).into_expr());
    }
    if s.starts_with("translate(") {
        let inner = extract_first_arg(s, "translate(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("translate needs column")?)?;
        let from_str = parts
            .get(1)
            .ok_or("translate needs from_str")?
            .trim_matches(['\'', '"']);
        let to_str = parts
            .get(2)
            .ok_or("translate needs to_str")?
            .trim_matches(['\'', '"']);
        let c = col(col_name);
        return Ok(robin_sparkless::translate(&c, from_str, to_str).into_expr());
    }
    if s.starts_with("substring_index(") {
        let inner = extract_first_arg(s, "substring_index(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("substring_index needs column")?)?;
        let delim = parts
            .get(1)
            .ok_or("substring_index needs delimiter")?
            .trim_matches(['\'', '"']);
        let count: i64 = parts
            .get(2)
            .ok_or("substring_index needs count")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let c = col(col_name);
        return Ok(robin_sparkless::substring_index(&c, delim, count).into_expr());
    }
    if s.starts_with("mask(") {
        let inner = extract_first_arg(s, "mask(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("mask needs column")?)?;
        let c = col(col_name);
        let parse_char = |i: usize| -> Option<char> {
            parts.get(i).and_then(|p| {
                let t = p.trim().trim_matches(['\'', '"']);
                if t.is_empty() {
                    None
                } else {
                    t.chars().next()
                }
            })
        };
        let upper = parse_char(1);
        let lower = parse_char(2);
        let digit = parse_char(3);
        let other = parse_char(4);
        return Ok(robin_sparkless::mask(&c, upper, lower, digit, other).into_expr());
    }
    if s.starts_with("soundex(") {
        let inner = extract_first_arg(s, "soundex(")?;
        let col_name = extract_col_name(inner.trim())?;
        let c = col(col_name);
        return Ok(c.soundex().into_expr());
    }
    if s.starts_with("levenshtein(") {
        let inner = extract_first_arg(s, "levenshtein(")?;
        let parts = parse_comma_separated_args(inner);
        let a_name = extract_col_name(parts.first().ok_or("levenshtein needs two columns")?)?;
        let b_name = extract_col_name(parts.get(1).ok_or("levenshtein needs two columns")?)?;
        let a_col = col(a_name);
        let b_col = col(b_name);
        return Ok(a_col.levenshtein(&b_col).into_expr());
    }
    if s.starts_with("crc32(") {
        let inner = extract_first_arg(s, "crc32(")?;
        let col_name = extract_col_name(inner.trim())?;
        let c = col(col_name);
        return Ok(c.crc32().into_expr());
    }
    if s.starts_with("xxhash64(") {
        let inner = extract_first_arg(s, "xxhash64(")?;
        let col_name = extract_col_name(inner.trim())?;
        let c = col(col_name);
        return Ok(c.xxhash64().into_expr());
    }
    if s.starts_with("get_json_object(") {
        let inner = extract_first_arg(s, "get_json_object(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("get_json_object needs column")?)?;
        let path = parts
            .get(1)
            .ok_or("get_json_object needs path")?
            .trim()
            .trim_matches(['\'', '"']);
        let c = col(col_name);
        return Ok(robin_sparkless::get_json_object(&c, path).into_expr());
    }

    // --- Math: sqrt, pow, exp, log ---
    if s.starts_with("sqrt(") {
        let inner = extract_first_arg(s, "sqrt(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(sqrt(&c).into_expr());
    }
    if s.starts_with("bround(") {
        let inner = extract_first_arg(s, "bround(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("bround needs column")?)?;
        let scale: i32 = parts
            .get(1)
            .ok_or("bround needs scale")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let c = col(col_name);
        return Ok(bround(&c, scale).into_expr());
    }
    if s.starts_with("negate(") || s.starts_with("negative(") {
        let prefix = if s.starts_with("negate(") {
            "negate("
        } else {
            "negative("
        };
        let inner = extract_first_arg(s, prefix)?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(negate(&c).into_expr());
    }
    if s.starts_with("positive(") {
        let inner = extract_first_arg(s, "positive(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(positive(&c).into_expr());
    }
    if s.starts_with("cot(") {
        let inner = extract_first_arg(s, "cot(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(cot(&c).into_expr());
    }
    if s.starts_with("csc(") {
        let inner = extract_first_arg(s, "csc(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(csc(&c).into_expr());
    }
    if s.starts_with("sec(") {
        let inner = extract_first_arg(s, "sec(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(sec(&c).into_expr());
    }
    if s == "e()" || s == "e" {
        return Ok(e().into_expr());
    }
    if s == "pi()" || s == "pi" {
        return Ok(pi().into_expr());
    }
    if s.starts_with("pow(") {
        let inner = extract_first_arg(s, "pow(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("pow needs column")?)?;
        let exp_val: i64 = parts
            .get(1)
            .ok_or("pow needs exponent")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let c = col(col_name);
        return Ok(pow(&c, exp_val).into_expr());
    }
    if s.starts_with("exp(") {
        let inner = extract_first_arg(s, "exp(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(exp(&c).into_expr());
    }
    if s.starts_with("log(") {
        let inner = extract_first_arg(s, "log(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(log(&c).into_expr());
    }
    // --- Math: sin, cos, tan, asin, acos, atan, atan2, degrees, radians, signum ---
    if s.starts_with("sin(") {
        let inner = extract_first_arg(s, "sin(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(sin(&col(col_name)).into_expr());
    }
    if s.starts_with("cos(") {
        let inner = extract_first_arg(s, "cos(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(cos(&col(col_name)).into_expr());
    }
    if s.starts_with("tan(") {
        let inner = extract_first_arg(s, "tan(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(tan(&col(col_name)).into_expr());
    }
    if s.starts_with("asin(") {
        let inner = extract_first_arg(s, "asin(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(asin(&col(col_name)).into_expr());
    }
    if s.starts_with("acos(") {
        let inner = extract_first_arg(s, "acos(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(acos(&col(col_name)).into_expr());
    }
    if s.starts_with("atan2(") {
        let inner = extract_first_arg(s, "atan2(")?;
        let parts = parse_comma_separated_args(inner);
        let y_name = extract_col_name(parts.first().ok_or("atan2 needs y")?)?;
        let x_name = extract_col_name(parts.get(1).ok_or("atan2 needs x")?)?;
        return Ok(atan2(&col(y_name), &col(x_name)).into_expr());
    }
    if s.starts_with("atan(") {
        let inner = extract_first_arg(s, "atan(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(atan(&col(col_name)).into_expr());
    }
    if s.starts_with("degrees(") {
        let inner = extract_first_arg(s, "degrees(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(degrees(&col(col_name)).into_expr());
    }
    if s.starts_with("radians(") {
        let inner = extract_first_arg(s, "radians(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(radians(&col(col_name)).into_expr());
    }
    if s.starts_with("signum(") {
        let inner = extract_first_arg(s, "signum(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(signum(&col(col_name)).into_expr());
    }
    if s.starts_with("cosh(") {
        let inner = extract_first_arg(s, "cosh(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(cosh(&col(col_name)).into_expr());
    }
    if s.starts_with("sinh(") {
        let inner = extract_first_arg(s, "sinh(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(sinh(&col(col_name)).into_expr());
    }
    if s.starts_with("tanh(") {
        let inner = extract_first_arg(s, "tanh(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(tanh(&col(col_name)).into_expr());
    }
    if s.starts_with("cbrt(") {
        let inner = extract_first_arg(s, "cbrt(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(cbrt(&col(col_name)).into_expr());
    }
    if s.starts_with("log10(") {
        let inner = extract_first_arg(s, "log10(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(log10(&col(col_name)).into_expr());
    }
    if s.starts_with("hypot(") {
        let inner = extract_first_arg(s, "hypot(")?;
        let parts = parse_comma_separated_args(inner);
        let col_x = extract_col_name(parts.first().ok_or("hypot needs x")?)?;
        let col_y = extract_col_name(parts.get(1).ok_or("hypot needs y")?)?;
        return Ok(hypot(&col(col_x), &col(col_y)).into_expr());
    }

    // --- Conditional/null: nvl, ifnull, nullif, nanvl ---
    if s.starts_with("nvl(") || s.starts_with("ifnull(") {
        let prefix = if s.starts_with("nvl(") {
            "nvl("
        } else {
            "ifnull("
        };
        let inner = extract_first_arg(s, prefix)?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("nvl/ifnull needs column")?)?;
        let col_expr =
            parse_column_or_literal(parts.get(1).ok_or("nvl/ifnull needs value")?.trim())?;
        let c = col(col_name);
        let val_col = robin_sparkless::Column::from_expr(col_expr, None);
        return Ok(nvl(&c, &val_col).into_expr());
    }
    if s.starts_with("nullif(") {
        let inner = extract_first_arg(s, "nullif(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("nullif needs column")?)?;
        let value_expr = parse_column_or_literal(parts.get(1).ok_or("nullif needs value")?.trim())?;
        let c = col(col_name);
        let val_col = robin_sparkless::Column::from_expr(value_expr, None);
        return Ok(nullif(&c, &val_col).into_expr());
    }
    if s.starts_with("nanvl(") {
        let inner = extract_first_arg(s, "nanvl(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("nanvl needs column")?)?;
        let value_expr = parse_column_or_literal(parts.get(1).ok_or("nanvl needs value")?.trim())?;
        let c = col(col_name);
        let val_col = robin_sparkless::Column::from_expr(value_expr, None);
        return Ok(nanvl(&c, &val_col).into_expr());
    }

    // --- Datetime: date_add, date_sub, hour, minute, second, datediff, last_day, trunc ---
    // (current_date/curdate/now/current_timestamp handled above with mock_dates)
    if s.starts_with("date_add(") {
        let inner = extract_first_arg(s, "date_add(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("date_add needs column")?)?;
        let n: i32 = parts
            .get(1)
            .ok_or("date_add needs n")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let c = col(col_name);
        return Ok(date_add(&c, n).into_expr());
    }
    if s.starts_with("date_sub(") {
        let inner = extract_first_arg(s, "date_sub(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("date_sub needs column")?)?;
        let n: i32 = parts
            .get(1)
            .ok_or("date_sub needs n")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let c = col(col_name);
        return Ok(date_sub(&c, n).into_expr());
    }
    if s.starts_with("hour(") {
        let inner = extract_first_arg(s, "hour(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(hour(&c).into_expr());
    }
    if s.starts_with("minute(") {
        let inner = extract_first_arg(s, "minute(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(minute(&c).into_expr());
    }
    if s.starts_with("second(") {
        let inner = extract_first_arg(s, "second(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(second(&c).into_expr());
    }
    if s.starts_with("datediff(") {
        let inner = extract_first_arg(s, "datediff(")?;
        let parts = parse_comma_separated_args(inner);
        let end_name = extract_col_name(parts.first().ok_or("datediff needs end column")?)?;
        let start_name = extract_col_name(parts.get(1).ok_or("datediff needs start column")?)?;
        let end_c = col(end_name);
        let start_c = col(start_name);
        return Ok(datediff(&end_c, &start_c).into_expr());
    }
    if s.starts_with("last_day(") {
        let inner = extract_first_arg(s, "last_day(")?;
        let col_name = extract_col_name(inner)?;
        let c = col(col_name);
        return Ok(last_day(&c).into_expr());
    }
    if s.starts_with("trunc(") {
        let inner = extract_first_arg(s, "trunc(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("trunc needs column")?)?;
        let format = parts
            .get(1)
            .ok_or("trunc needs format")?
            .trim_matches(['\'', '"']);
        let c = col(col_name);
        return Ok(trunc(&c, format).into_expr());
    }
    if s.starts_with("quarter(") {
        let inner = extract_first_arg(s, "quarter(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(quarter(&col(col_name)).into_expr());
    }
    if s.starts_with("weekofyear(") || s.starts_with("week(") {
        let prefix = if s.starts_with("weekofyear(") {
            "weekofyear("
        } else {
            "week("
        };
        let inner = extract_first_arg(s, prefix)?;
        let col_name = extract_col_name(inner)?;
        return Ok(weekofyear(&col(col_name)).into_expr());
    }
    if s.starts_with("dayofweek(") {
        let inner = extract_first_arg(s, "dayofweek(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(dayofweek(&col(col_name)).into_expr());
    }
    if s.starts_with("dayofyear(") {
        let inner = extract_first_arg(s, "dayofyear(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(dayofyear(&col(col_name)).into_expr());
    }
    if s.starts_with("add_months(") {
        let inner = extract_first_arg(s, "add_months(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("add_months needs column")?)?;
        let n: i32 = parts
            .get(1)
            .ok_or("add_months needs n")?
            .trim()
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        return Ok(add_months(&col(col_name), n).into_expr());
    }
    if s.starts_with("months_between(") {
        let inner = extract_first_arg(s, "months_between(")?;
        let parts = parse_comma_separated_args(inner);
        let end_name = extract_col_name(parts.first().ok_or("months_between needs end column")?)?;
        let start_name =
            extract_col_name(parts.get(1).ok_or("months_between needs start column")?)?;
        let round_off = parts
            .get(2)
            .map(|p| matches!(p.trim().to_lowercase().as_str(), "true" | "1"))
            .unwrap_or(true);
        return Ok(months_between(&col(end_name), &col(start_name), round_off).into_expr());
    }
    if s.starts_with("next_day(") {
        let inner = extract_first_arg(s, "next_day(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("next_day needs column")?)?;
        let day_str = parts
            .get(1)
            .ok_or("next_day needs day of week")?
            .trim()
            .trim_matches(['\'', '"']);
        return Ok(next_day(&col(col_name), day_str).into_expr());
    }

    // --- Type/conditional: cast, try_cast, isnan, greatest, least ---
    if s.starts_with("cast(") {
        let inner = extract_first_arg(s, "cast(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("cast needs column")?)?;
        let type_str = parts
            .get(1)
            .ok_or("cast needs type name")?
            .trim()
            .trim_matches(['\'', '"']);
        let c = col(col_name);
        return Ok(cast(&c, type_str).map_err(|e| e.to_string())?.into_expr());
    }
    if s.starts_with("try_cast(") {
        let inner = extract_first_arg(s, "try_cast(")?;
        let parts = parse_comma_separated_args(inner);
        let col_name = extract_col_name(parts.first().ok_or("try_cast needs column")?)?;
        let type_str = parts
            .get(1)
            .ok_or("try_cast needs type name")?
            .trim()
            .trim_matches(['\'', '"']);
        let c = col(col_name);
        return Ok(try_cast(&c, type_str)
            .map_err(|e| e.to_string())?
            .into_expr());
    }
    if s.starts_with("isnan(") {
        let inner = extract_first_arg(s, "isnan(")?;
        let col_name = extract_col_name(inner)?;
        return Ok(isnan(&col(col_name)).into_expr());
    }
    if s.starts_with("greatest(") {
        let inner = extract_first_arg(s, "greatest(")?;
        let parts = parse_comma_separated_args(inner);
        let cols: Vec<_> = parts
            .iter()
            .map(|p| extract_col_name(p).map(col))
            .collect::<Result<Vec<_>, _>>()?;
        let col_refs: Vec<&robin_sparkless::Column> = cols.iter().collect();
        return Ok(greatest(&col_refs).map_err(|e| e.to_string())?.into_expr());
    }
    if s.starts_with("least(") {
        let inner = extract_first_arg(s, "least(")?;
        let parts = parse_comma_separated_args(inner);
        let cols: Vec<_> = parts
            .iter()
            .map(|p| extract_col_name(p).map(col))
            .collect::<Result<Vec<_>, _>>()?;
        let col_refs: Vec<&robin_sparkless::Column> = cols.iter().collect();
        return Ok(least(&col_refs).map_err(|e| e.to_string())?.into_expr());
    }

    // Try to parse as a general expression that can include arithmetic, comparisons, and logical operators.
    // This unified parser handles expressions like:
    // - Boolean: col('age') > 30 AND col('score') < 100
    // - Arithmetic: col('a') + col('b')
    // - Mixed: (col('a') + col('b')) > col('c')
    fn parse_general_expr(src: &str) -> Result<Expr, String> {
        fn trim_outer_parens(s: &str) -> &str {
            let mut s = s.trim();
            loop {
                if !s.starts_with('(') || !s.ends_with(')') {
                    return s;
                }
                let mut depth = 0i32;
                let mut matched = false;
                for (i, ch) in s.char_indices() {
                    match ch {
                        '(' => depth += 1,
                        ')' => {
                            depth -= 1;
                            if depth == 0 {
                                // If the first '(' closes before the end, these aren't outer parens.
                                if i != s.len() - 1 {
                                    matched = false;
                                    break;
                                }
                                matched = true;
                            }
                        }
                        _ => {}
                    }
                }
                if matched {
                    s = &s[1..s.len() - 1];
                    s = s.trim();
                } else {
                    return s;
                }
            }
        }

        fn find_comparison_op(s: &str) -> Option<(usize, usize, &'static str)> {
            // Find the first comparison operator at top level (outside quotes/parentheses).
            // Supports: >=, <=, ==, !=, >, <, =
            let bytes = s.as_bytes();
            let slen = s.len();
            let mut depth = 0i32;
            let mut in_single = false;
            let mut in_double = false;

            let mut i = 0usize;
            while i < slen {
                let ch = bytes[i] as char;
                match ch {
                    '\'' if !in_double => in_single = !in_single,
                    '"' if !in_single => in_double = !in_double,
                    '(' if !in_single && !in_double => depth += 1,
                    ')' if !in_single && !in_double => depth -= 1,
                    _ => {}
                }

                if depth == 0 && !in_single && !in_double {
                    if i + 2 <= slen {
                        match &bytes[i..i + 2] {
                            b">=" => return Some((i, 2, ">=")),
                            b"<=" => return Some((i, 2, "<=")),
                            b"==" => return Some((i, 2, "==")),
                            b"!=" => return Some((i, 2, "!=")),
                            _ => {}
                        }
                    }
                    match bytes[i] {
                        b'>' => return Some((i, 1, ">")),
                        b'<' => return Some((i, 1, "<")),
                        b'=' => return Some((i, 1, "=")),
                        _ => {}
                    }
                }

                i += 1;
            }
            None
        }

        fn split_on_ops(s: &str, ops: &[char]) -> Option<(String, char, String)> {
            let mut depth = 0i32;
            let mut in_single = false;
            let mut in_double = false;
            let chars: Vec<char> = s.chars().collect();
            for i in 0..chars.len() {
                let ch = chars[i];
                match ch {
                    '\'' if !in_double => in_single = !in_single,
                    '"' if !in_single => in_double = !in_double,
                    '(' if !in_single && !in_double => depth += 1,
                    ')' if !in_single && !in_double => depth -= 1,
                    _ => {}
                }
                if depth == 0 && !in_single && !in_double && ops.contains(&ch) {
                    let left: String = chars[..i].iter().collect();
                    let right: String = chars[i + 1..].iter().collect();
                    return Some((left, ch, right));
                }
            }
            None
        }

        let s = trim_outer_parens(src);
        let s_trimmed = s.trim();

        // First, try to parse as a boolean/logical expression (reuse existing parser)
        if let Ok(expr) = parse_simple_filter_expr(s_trimmed, None) {
            return Ok(expr);
        }

        // If that fails, try to find a comparison operator
        // This handles cases like: (col('a') + col('b')) > col('c')
        if let Some((idx, len, op)) = find_comparison_op(s_trimmed) {
            let left_str = &s_trimmed[..idx].trim();
            let right_str = &s_trimmed[idx + len..].trim();

            // Parse left and right sides as general expressions (can be arithmetic, columns, or literals)
            let left_expr = parse_general_expr(left_str)?;
            let right_expr = parse_general_expr(right_str)?;

            // Use standard Polars comparisons (they handle type coercion automatically)
            return Ok(match op {
                ">" => left_expr.gt(right_expr),
                ">=" => left_expr.gt_eq(right_expr),
                "<" => left_expr.lt(right_expr),
                "<=" => left_expr.lt_eq(right_expr),
                "==" | "=" => left_expr.eq(right_expr),
                "!=" => left_expr.neq(right_expr),
                _ => return Err(format!("unsupported comparison operator: {op}")),
            });
        }

        // No comparison operator found - try arithmetic
        // Lowest precedence: + and -
        if let Some((left, op, right)) = split_on_ops(s_trimmed, &['+', '-']) {
            let lhs = parse_general_expr(&left)?;
            let rhs = parse_general_expr(&right)?;
            use std::ops::{Add, Sub};
            return Ok(match op {
                '+' => lhs.add(rhs),
                '-' => lhs.sub(rhs),
                _ => unreachable!(),
            });
        }

        // Higher precedence: * and /
        if let Some((left, op, right)) = split_on_ops(s_trimmed, &['*', '/']) {
            let lhs = parse_general_expr(&left)?;
            let rhs = parse_general_expr(&right)?;
            use std::ops::{Div, Mul};
            return Ok(match op {
                '*' => lhs.mul(rhs),
                '/' => lhs.div(rhs),
                _ => unreachable!(),
            });
        }

        // Leaf: column or literal
        parse_column_or_literal(s_trimmed)
    }

    // Try the unified parser
    if let Ok(expr) = parse_general_expr(s) {
        return Ok(expr);
    }

    Err(format!("unsupported withColumn expression: {s}"))
}

/// Parse a column reference or literal value
fn parse_column_or_literal(s: &str) -> Result<Expr, String> {
    use robin_sparkless::{col, lit_i64, lit_str};
    let s = s.trim();

    if s.starts_with("col(") {
        // Only accept *exactly* `col('name')` / `col("name")` here.
        // If the matching ')' for the first '(' isn't at the end, this is not a simple column ref.
        let bytes = s.as_bytes();
        let mut depth = 0i32;
        let mut in_single = false;
        let mut in_double = false;
        let mut close_idx: Option<usize> = None;
        for (i, &b) in bytes.iter().enumerate() {
            let ch = b as char;
            match ch {
                '\'' if !in_double => in_single = !in_single,
                '"' if !in_single => in_double = !in_double,
                '(' if !in_single && !in_double => depth += 1,
                ')' if !in_single && !in_double => {
                    depth -= 1;
                    if depth == 0 {
                        close_idx = Some(i);
                        break;
                    }
                }
                _ => {}
            }
        }

        let close_idx = close_idx.ok_or_else(|| format!("invalid col(...) expression: {s}"))?;
        if close_idx != s.len() - 1 {
            return Err(format!("invalid col(...) reference (trailing tokens): {s}"));
        }

        let inner = s[4..s.len() - 1].trim();
        if (inner.starts_with('\'') && inner.ends_with('\''))
            || (inner.starts_with('"') && inner.ends_with('"'))
        {
            let col_name = inner.trim_matches(['\'', '"']);
            Ok(col(col_name).into_expr())
        } else {
            Err(format!("col(...) must wrap a quoted column name, got: {s}"))
        }
    } else if s.starts_with("lit(") {
        use robin_sparkless::lit_f64;
        let lit_content = s[4..s.len() - 1].trim();
        // Handle lit(None) for null literals
        if lit_content == "None" {
            use polars::prelude::*;
            // Create an expression that is always a null Int64 value
            Ok(lit(NULL).cast(DataType::Int64))
        } else {
            let lit_val = lit_content.trim_matches(['\'', '"']);
            // Try to parse as number, otherwise treat as string
            if let Ok(num) = lit_val.parse::<i64>() {
                Ok(lit_i64(num).into_expr())
            } else if let Ok(num) = lit_val.parse::<f64>() {
                Ok(lit_f64(num).into_expr())
            } else {
                Ok(lit_str(lit_val).into_expr())
            }
        }
    } else if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"'))
    {
        // Quoted string literal - remove outer quotes
        let val = s.trim_matches(['\'', '"']);
        Ok(lit_str(val).into_expr())
    } else if s.starts_with('(') && (s.ends_with('\'') || s.ends_with('"')) {
        // Handle case like "('minor'" - remove leading ( and quotes
        let val = s.trim_start_matches('(').trim_matches(['\'', '"']);
        Ok(lit_str(val).into_expr())
    } else if let Ok(num) = s.parse::<i64>() {
        // Numeric literal
        Ok(lit_i64(num).into_expr())
    } else {
        // Treat as string literal
        Ok(lit_str(s).into_expr())
    }
}

/// Parse string as JSON, handling both JSON and Python repr format (single quotes).
/// Returns Ok(Value) if parseable, Err otherwise.
fn parse_json_like_string(s: &str) -> Result<Value, serde_json::Error> {
    serde_json::from_str(s).or_else(|_| {
        // Python repr uses single quotes; try replacing for keys/values
        let normalized: String = s
            .replace("'", "\"")
            .replace("None", "null")
            .replace("True", "true")
            .replace("False", "false");
        serde_json::from_str(&normalized)
    })
}

/// Normalize timestamp string for parity comparison: strip trailing .000000 when fractional part is zero (PySpark format).
fn normalize_timestamp_str(s: &str) -> String {
    if s.ends_with(".000000") {
        s.strip_suffix(".000000").unwrap_or(s).to_string()
    } else {
        s.to_string()
    }
}

/// Convert Polars AnyValue to serde_json Value (for list elements and scalars).
fn any_value_to_json(av: &polars::prelude::AnyValue, _dtype: &polars::prelude::DataType) -> Value {
    use polars::prelude::AnyValue;
    use serde_json::{Map, Number};
    match av {
        AnyValue::Null => Value::Null,
        AnyValue::Boolean(v) => Value::Bool(*v),
        AnyValue::Int32(v) => Value::Number((*v).into()),
        AnyValue::Int64(v) => Value::Number((*v).into()),
        AnyValue::UInt32(v) => Value::Number((*v).into()),
        AnyValue::Float64(v) => Number::from_f64(*v)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        AnyValue::String(v) => Value::String(v.to_string()),
        AnyValue::List(s) => {
            let arr: Vec<Value> = (0..s.len())
                .filter_map(|i| s.get(i).ok())
                .map(|a| any_value_to_json(&a, s.dtype()))
                .collect();
            Value::Array(arr)
        }
        AnyValue::Struct(_, _, fields) => {
            let mut obj = Map::new();
            for (fld_av, fld) in av._iter_struct_av().zip(fields.iter()) {
                obj.insert(fld.name.to_string(), any_value_to_json(&fld_av, &fld.dtype));
            }
            Value::Object(obj)
        }
        AnyValue::StructOwned(payload) => {
            let (values, fields) = &**payload;
            let mut obj = Map::new();
            for (av, fld) in values.iter().zip(fields.iter()) {
                obj.insert(fld.name.to_string(), any_value_to_json(av, &fld.dtype));
            }
            Value::Object(obj)
        }
        _ => Value::String(format!("{av:?}")),
    }
}

/// Check if List inner dtype is Struct with "key" and "value" fields (map format).
fn is_map_format(dtype: &polars::prelude::DataType) -> bool {
    if let polars::prelude::DataType::List(inner) = dtype {
        if let polars::prelude::DataType::Struct(fields) = inner.as_ref() {
            let has_key = fields.iter().any(|f| f.name == "key");
            let has_value = fields.iter().any(|f| f.name == "value");
            return has_key && has_value;
        }
    }
    false
}

/// Convert List(Struct{key, value}) to JSON object {key: value, ...}.
fn list_of_key_value_struct_to_object(list_series: &polars::prelude::Series) -> serde_json::Value {
    use polars::prelude::AnyValue;
    use serde_json::Map;
    let mut obj = Map::new();
    for i in 0..list_series.len() {
        if let Ok(av) = list_series.get(i) {
            let (key_val, val_val) = match &av {
                AnyValue::Struct(_, _, fields) => {
                    let mut k = None;
                    let mut v = None;
                    for (fld_av, fld) in av._iter_struct_av().zip(fields.iter()) {
                        if fld.name == "key" {
                            k = fld_av
                                .get_str()
                                .map(|s| s.to_string())
                                .or_else(|| Some(fld_av.to_string()));
                        } else if fld.name == "value" {
                            v = Some(any_value_to_json(&fld_av, &fld.dtype));
                        }
                    }
                    (k, v)
                }
                AnyValue::StructOwned(payload) => {
                    let (values, fields) = &**payload;
                    let mut k = None;
                    let mut v = None;
                    for (fld_av, fld) in values.iter().zip(fields.iter()) {
                        if fld.name == "key" {
                            k = fld_av
                                .get_str()
                                .map(|s| s.to_string())
                                .or_else(|| Some(fld_av.to_string()));
                        } else if fld.name == "value" {
                            v = Some(any_value_to_json(fld_av, &fld.dtype));
                        }
                    }
                    (k, v)
                }
                _ => (None, None),
            };
            if let (Some(key), Some(val)) = (key_val, val_val) {
                obj.insert(key, val);
            }
        }
    }
    Value::Object(obj)
}

/// Collect a DataFrame to a simple (schema, rows) representation for comparison.
fn collect_to_simple_format(
    df: &DataFrame,
) -> Result<(Vec<ColumnSpec>, Vec<Vec<Value>>), PolarsError> {
    let pl_df = df.collect()?;
    let schema = pl_df.schema();

    // Build schema
    let col_specs: Vec<ColumnSpec> = schema
        .iter()
        .map(|(name, dtype)| ColumnSpec {
            name: name.to_string(),
            r#type: dtype_to_string(dtype),
        })
        .collect();

    // Build rows
    let num_rows = pl_df.height();
    let num_cols = schema.len();
    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(num_rows);

    // Extract rows by iterating through each column
    for row_idx in 0..num_rows {
        let mut row: Vec<Value> = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let series = pl_df.get_columns().get(col_idx).ok_or_else(|| {
                PolarsError::ComputeError(format!("column index {col_idx} out of range").into())
            })?;
            let json_val = match series.get(row_idx) {
                Ok(av) => {
                    // Check for null first
                    if matches!(av, polars::prelude::AnyValue::Null) {
                        Value::Null
                    } else if matches!(series.dtype(), polars::prelude::DataType::String) {
                        // For String type, extract the actual string value
                        let debug_str = format!("{av:?}");
                        // Handle "StringOwned(\"value\")" format
                        if debug_str.starts_with("StringOwned(") && debug_str.ends_with(")") {
                            let inner = &debug_str[12..debug_str.len() - 1];
                            // Remove outer quotes if present
                            let cleaned = inner.trim_matches('"');
                            Value::String(normalize_timestamp_str(cleaned))
                        } else if debug_str.starts_with('"') && debug_str.ends_with('"') {
                            // Handle quoted strings
                            Value::String(normalize_timestamp_str(
                                &debug_str[1..debug_str.len() - 1],
                            ))
                        } else {
                            // Try to match known variants
                            match av {
                                polars::prelude::AnyValue::String(v) => {
                                    Value::String(normalize_timestamp_str(v))
                                }
                                _ => Value::String(debug_str),
                            }
                        }
                    } else if matches!(series.dtype(), polars::prelude::DataType::List(_)) {
                        // List/array column: convert to JSON array, or object if map (List<Struct{key,value}>)
                        match av {
                            polars::prelude::AnyValue::Null => Value::Null,
                            polars::prelude::AnyValue::List(s) => {
                                if is_map_format(series.dtype()) {
                                    list_of_key_value_struct_to_object(&s)
                                } else {
                                    let arr: Vec<Value> = (0..s.len())
                                        .filter_map(|i| s.get(i).ok())
                                        .map(|a| any_value_to_json(&a, s.dtype()))
                                        .collect();
                                    Value::Array(arr)
                                }
                            }
                            _ => Value::Null,
                        }
                    } else if matches!(series.dtype(), polars::prelude::DataType::Struct(_)) {
                        // Struct column: convert to JSON object
                        match av {
                            polars::prelude::AnyValue::Null => Value::Null,
                            _ => any_value_to_json(&av, series.dtype()),
                        }
                    } else {
                        // For non-string types, use standard matching
                        match av {
                            polars::prelude::AnyValue::Null => Value::Null,
                            polars::prelude::AnyValue::Boolean(v) => Value::Bool(v),
                            polars::prelude::AnyValue::Int64(v) => Value::Number(v.into()),
                            polars::prelude::AnyValue::Int32(v) => Value::Number(v.into()),
                            polars::prelude::AnyValue::Int8(v) => Value::Number((v as i64).into()),
                            polars::prelude::AnyValue::UInt32(v) => Value::Number(v.into()),
                            polars::prelude::AnyValue::Float64(v) => {
                                // Convert f64 to JSON Number
                                use serde_json::Number;
                                if let Some(n) = Number::from_f64(v) {
                                    Value::Number(n)
                                } else {
                                    Value::Null
                                }
                            }
                            polars::prelude::AnyValue::String(v) => Value::String(v.to_string()),
                            polars::prelude::AnyValue::Date(days) => {
                                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                                let d = epoch + chrono::TimeDelta::days(days as i64);
                                Value::String(d.format("%Y-%m-%d").to_string())
                            }
                            polars::prelude::AnyValue::Datetime(us, tu, _)
                            | polars::prelude::AnyValue::DatetimeOwned(us, tu, _) => {
                                let micros = match tu {
                                    TimeUnit::Microseconds => us,
                                    TimeUnit::Milliseconds => us.saturating_mul(1000),
                                    TimeUnit::Nanoseconds => us.saturating_div(1000),
                                };
                                let dt = chrono::DateTime::from_timestamp_micros(micros)
                                    .unwrap_or_default();
                                let formatted = dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string();
                                Value::String(normalize_timestamp_str(&formatted))
                            }
                            _ => {
                                // For unknown types, try to extract as number if dtype suggests it
                                if matches!(series.dtype(), polars::prelude::DataType::UInt32) {
                                    // Try to get the value as u32 from debug format
                                    let debug_str = format!("{av:?}");
                                    if let Some(start) = debug_str.find('(') {
                                        if let Some(end) = debug_str.rfind(')') {
                                            if let Ok(num) =
                                                debug_str[start + 1..end].parse::<u32>()
                                            {
                                                Value::Number(num.into())
                                            } else {
                                                Value::String(debug_str)
                                            }
                                        } else {
                                            Value::String(debug_str)
                                        }
                                    } else {
                                        Value::String(debug_str)
                                    }
                                } else {
                                    Value::String(format!("{av:?}"))
                                }
                            }
                        }
                    }
                }
                Err(_) => Value::Null,
            };
            row.push(json_val);
        }
        rows.push(row);
    }

    Ok((col_specs, rows))
}

/// Convert Polars DataType to a simple string representation.
fn dtype_to_string(dtype: &polars::prelude::DataType) -> String {
    match dtype {
        polars::prelude::DataType::Int64 => "bigint".to_string(),
        polars::prelude::DataType::Int32 => "int".to_string(),
        polars::prelude::DataType::Int8 => "Int8".to_string(),
        polars::prelude::DataType::UInt32 => "UInt32".to_string(),
        polars::prelude::DataType::String => "string".to_string(),
        polars::prelude::DataType::Float64 => "Float64".to_string(),
        polars::prelude::DataType::Boolean => "boolean".to_string(),
        polars::prelude::DataType::Date => "date".to_string(),
        polars::prelude::DataType::Datetime(_, _) => "timestamp".to_string(),
        polars::prelude::DataType::List(inner) => format!("array<{}>", dtype_to_string(inner)),
        _ => format!("{dtype:?}"),
    }
}

/// Assert that schemas match (column names and types).
/// When `join_fixture` is true, allows actual names with "_right" suffix to match expected
/// duplicate names (PySpark keeps duplicate column names; Polars uses "_right" suffix).
fn assert_schema_eq(
    actual: &[ColumnSpec],
    expected: &[ColumnSpec],
    fixture_name: &str,
    join_fixture: bool,
) -> Result<(), PolarsError> {
    if actual.len() != expected.len() {
        return Err(PolarsError::ComputeError(
            format!(
                "fixture {}: schema length mismatch: actual {} columns, expected {}",
                fixture_name,
                actual.len(),
                expected.len()
            )
            .into(),
        ));
    }

    let mut expected_name_occurrence: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

    for (i, (act, exp)) in actual.iter().zip(expected.iter()).enumerate() {
        let occurrence = expected_name_occurrence
            .entry(exp.name.clone())
            .or_insert(0);
        *occurrence += 1;
        let is_duplicate = *occurrence > 1;

        let names_match = if join_fixture && is_duplicate {
            act.name == format!("{}_right", exp.name) || act.name == exp.name
        } else {
            act.name == exp.name
        };
        if !names_match {
            return Err(PolarsError::ComputeError(
                format!(
                    "fixture {}: column {} name mismatch: actual '{}', expected '{}'",
                    fixture_name, i, act.name, exp.name
                )
                .into(),
            ));
        }
        // Type comparison is lenient for now (int vs bigint both OK)
        if !types_compatible(&act.r#type, &exp.r#type) {
            return Err(PolarsError::ComputeError(
                format!(
                    "fixture {}: column '{}' type mismatch: actual '{}', expected '{}'",
                    fixture_name, act.name, act.r#type, exp.r#type
                )
                .into(),
            ));
        }
    }

    Ok(())
}

/// Check if two type strings are compatible (lenient matching).
fn types_compatible(actual: &str, expected: &str) -> bool {
    if actual == expected {
        return true;
    }
    // Struct: actual is "Struct([...])", expected may be "struct" or "struct<...>"
    if actual.starts_with("Struct(")
        && (expected == "struct"
            || expected.starts_with("struct<")
            || expected.starts_with("Struct("))
    {
        return true;
    }
    // Map: we use array<Struct([...])>, expected may be "map"
    if actual.starts_with("array<Struct(") && expected == "map" {
        return true;
    }
    // Allow int/bigint/long/integer/Int8 to match (hour/minute return Int8 in Polars)
    let int_types = ["int", "integer", "bigint", "long", "Int8"];
    if int_types.contains(&actual) && int_types.contains(&expected) {
        return true;
    }
    // Float64/double/float (median, etc.)
    let float_types = ["Float64", "double", "float"];
    if float_types.contains(&actual) && float_types.contains(&expected) {
        return true;
    }
    // Allow UInt32/uint32 to match with each other or with bigint (common for count/length)
    if (actual == "UInt32" || actual == "uint32") && (expected == "UInt32" || expected == "uint32")
    {
        return true;
    }
    if (actual == "UInt32" || actual == "uint32") && int_types.contains(&expected) {
        return true;
    }
    if (expected == "UInt32" || expected == "uint32") && int_types.contains(&actual) {
        return true;
    }
    // Allow string/str/varchar to match
    let string_types = ["string", "str", "varchar"];
    if string_types.contains(&actual) && string_types.contains(&expected) {
        return true;
    }
    // Allow Float64/double to match (for avg operations)
    let float_types = ["Float64", "double", "float"];
    if float_types.contains(&actual) && float_types.contains(&expected) {
        return true;
    }
    // Allow array<...> types to match (list columns)
    if actual.starts_with("array<") && expected.starts_with("array<") {
        return true;
    }
    // Allow boolean/bool to match
    let bool_types = ["boolean", "bool", "Boolean"];
    if bool_types.contains(&actual) && bool_types.contains(&expected) {
        return true;
    }
    // Allow date/Date to match
    let date_types = ["date", "Date"];
    if date_types.contains(&actual) && date_types.contains(&expected) {
        return true;
    }
    // Allow timestamp/datetime/timestamp_ntz to match
    let ts_types = ["timestamp", "datetime", "timestamp_ntz", "TimestampType"];
    if ts_types.contains(&actual) && ts_types.contains(&expected) {
        return true;
    }
    // PySpark assert_true returns void; we return boolean.
    if (expected == "void") && (actual == "boolean" || actual == "bool" || actual == "Boolean") {
        return true;
    }
    false
}

/// Assert that rows match (with optional ordering requirement).
/// When expected_schema is provided, struct columns can be compared as object (actual) vs array (expected).
fn assert_rows_eq(
    actual: &[Vec<Value>],
    expected: &[Vec<Value>],
    ordered: bool,
    fixture_name: &str,
    expected_schema: Option<&[ColumnSpec]>,
) -> Result<(), PolarsError> {
    if actual.len() != expected.len() {
        return Err(PolarsError::ComputeError(
            format!(
                "fixture {}: row count mismatch: actual {} rows, expected {}",
                fixture_name,
                actual.len(),
                expected.len()
            )
            .into(),
        ));
    }

    let mut actual_sorted = actual.to_vec();
    let mut expected_sorted = expected.to_vec();

    if !ordered {
        actual_sorted.sort_by(|a, b| compare_rows(a, b));
        expected_sorted.sort_by(|a, b| compare_rows(a, b));
    }

    for (i, (act_row, exp_row)) in actual_sorted.iter().zip(expected_sorted.iter()).enumerate() {
        if act_row.len() != exp_row.len() {
            return Err(PolarsError::ComputeError(
                format!(
                    "fixture {}: row {} length mismatch: actual {} values, expected {}",
                    fixture_name,
                    i,
                    act_row.len(),
                    exp_row.len()
                )
                .into(),
            ));
        }

        for (j, (act_val, exp_val)) in act_row.iter().zip(exp_row.iter()).enumerate() {
            let col_spec = expected_schema.and_then(|s| s.get(j));
            if !values_equal_with_schema(act_val, exp_val, col_spec) {
                return Err(PolarsError::ComputeError(
                    format!(
                        "fixture {fixture_name}: row {i}, column {j} mismatch: actual {act_val:?}, expected {exp_val:?}"
                    )
                    .into(),
                ));
            }
        }
    }

    Ok(())
}

/// Compare two rows for sorting (lexicographic).
fn compare_rows(a: &[Value], b: &[Value]) -> std::cmp::Ordering {
    for (av, bv) in a.iter().zip(b.iter()) {
        let ord = compare_values(av, bv);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    a.len().cmp(&b.len())
}

/// Compare two JSON Values for ordering.
fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        (Value::Number(n1), Value::Number(n2)) => {
            if let (Some(i1), Some(i2)) = (n1.as_i64(), n2.as_i64()) {
                i1.cmp(&i2)
            } else if let (Some(f1), Some(f2)) = (n1.as_f64(), n2.as_f64()) {
                f1.partial_cmp(&f2).unwrap_or(std::cmp::Ordering::Equal)
            } else {
                n1.to_string().cmp(&n2.to_string())
            }
        }
        (Value::String(s1), Value::String(s2)) => s1.cmp(s2),
        (Value::Bool(b1), Value::Bool(b2)) => b1.cmp(b2),
        (Value::Array(a1), Value::Array(a2)) => {
            for (x, y) in a1.iter().zip(a2.iter()) {
                let ord = compare_values(x, y);
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            a1.len().cmp(&a2.len())
        }
        _ => format!("{a:?}").cmp(&format!("{b:?}")),
    }
}

/// Parse struct type string "struct<foo:int,bar:int>" to get field names in order (simple, no nested structs).
fn struct_field_names(type_str: &str) -> Option<Vec<String>> {
    let s = type_str.trim();
    if !s.to_lowercase().starts_with("struct<") || !s.ends_with('>') {
        return None;
    }
    let inner = s[7..s.len() - 1].trim(); // between struct< and >
    if inner.is_empty() {
        return Some(Vec::new());
    }
    let mut names = Vec::new();
    for part in inner.split(',') {
        let part = part.trim();
        if let Some(idx) = part.find(':') {
            names.push(part[..idx].trim().to_string());
        }
    }
    Some(names)
}

/// Convert JSON Object to Array using field order (for struct comparison when expected is array).
fn struct_object_to_array(obj: &serde_json::Map<String, Value>, field_names: &[String]) -> Value {
    let arr: Vec<Value> = field_names
        .iter()
        .filter_map(|k| obj.get(k).cloned())
        .collect();
    Value::Array(arr)
}

fn values_equal_with_struct(a: &Value, b: &Value, struct_fields: Option<&[String]>) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Number(n1), Value::Number(n2)) => {
            if let (Some(i1), Some(i2)) = (n1.as_i64(), n2.as_i64()) {
                i1 == i2
            } else if let (Some(f1), Some(f2)) = (n1.as_f64(), n2.as_f64()) {
                (f1 - f2).abs() < 1e-10
            } else {
                false
            }
        }
        (Value::String(s1), Value::String(s2)) => {
            // Compare JSON-like strings (array/map) structurally; Polars uses [1,2,3], PySpark uses [1, 2, 3]
            if (s1.starts_with('[') && s2.starts_with('['))
                || (s1.starts_with('{') && s2.starts_with('{'))
            {
                if let (Ok(v1), Ok(v2)) = (parse_json_like_string(s1), parse_json_like_string(s2)) {
                    return values_equal_with_struct(&v1, &v2, None);
                }
            }
            normalize_timestamp_str(s1) == normalize_timestamp_str(s2)
        }
        (Value::Bool(b1), Value::Bool(b2)) => b1 == b2,
        (Value::Array(a1), Value::Array(a2)) => {
            a1.len() == a2.len()
                && a1
                    .iter()
                    .zip(a2.iter())
                    .all(|(x, y)| values_equal_with_struct(x, y, None))
        }
        (Value::Object(o1), Value::Object(o2)) => {
            if o1.len() != o2.len() {
                return false;
            }
            for (k, v1) in o1 {
                match o2.get(k) {
                    Some(v2) => {
                        if !values_equal_with_struct(v1, v2, None) {
                            return false;
                        }
                    }
                    None => return false,
                }
            }
            true
        }
        (Value::Object(o), Value::Array(arr)) | (Value::Array(arr), Value::Object(o)) => {
            if let Some(fields) = struct_fields {
                let arr_from_obj = struct_object_to_array(o, fields);
                values_equal_with_struct(&arr_from_obj, &Value::Array(arr.clone()), None)
            } else {
                false
            }
        }
        _ => false,
    }
}

fn values_equal_with_schema(a: &Value, b: &Value, col_schema: Option<&ColumnSpec>) -> bool {
    // PySpark assert_true returns void (null). We return boolean column of nulls; treat as equal when expected is void and null.
    if let Some(c) = col_schema {
        if c.r#type == "void" && b == &Value::Null {
            return a == &Value::Null;
        }
    }
    let struct_fields = col_schema.and_then(|c| struct_field_names(&c.r#type));
    values_equal_with_struct(a, b, struct_fields.as_deref())
}

/// Run with: cargo test print_rand_seed_42_values -- --ignored --nocapture
/// Plan fixture: schema + rows + plan + expected (see docs/LOGICAL_PLAN_FORMAT.md).
#[derive(Debug, Deserialize)]
struct PlanFixture {
    name: String,
    input: InputSection,
    plan: Vec<Value>,
    expected: ExpectedSection,
}

/// Run plan interpreter against fixtures in tests/fixtures/plans/.
#[test]
fn plan_parity_fixtures() {
    let plans_dir = Path::new("tests/fixtures/plans");
    if !plans_dir.exists() {
        return;
    }
    let mut failures = Vec::new();
    for entry in fs::read_dir(plans_dir).expect("read plans dir") {
        let path = entry.expect("entry").path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let text = fs::read_to_string(&path).expect("read fixture");
        let fixture: PlanFixture = serde_json::from_str(&text).expect("parse plan fixture");

        let spark = SparkSession::builder()
            .app_name("plan_parity")
            .get_or_create();

        let schema: Vec<(String, String)> = fixture
            .input
            .schema
            .iter()
            .map(|s| (s.name.clone(), s.r#type.clone()))
            .collect();
        let rows = fixture.input.rows.clone();

        match plan::execute_plan(&spark, rows, schema, &fixture.plan) {
            Ok(result_df) => {
                let (actual_schema, actual_rows) = match collect_to_simple_format(&result_df) {
                    Ok(t) => t,
                    Err(e) => {
                        failures.push((fixture.name.clone(), format!("collect: {e}")));
                        continue;
                    }
                };
                if let Err(e) = assert_schema_eq(
                    &actual_schema,
                    &fixture.expected.schema,
                    &fixture.name,
                    false,
                ) {
                    failures.push((fixture.name.clone(), e.to_string()));
                    continue;
                }
                let ordered = fixture
                    .plan
                    .iter()
                    .any(|op| op.get("op").and_then(Value::as_str) == Some("orderBy"));
                if let Err(e) = assert_rows_eq(
                    &actual_rows,
                    &fixture.expected.rows,
                    ordered,
                    &fixture.name,
                    Some(&fixture.expected.schema),
                ) {
                    failures.push((fixture.name.clone(), e.to_string()));
                }
            }
            Err(e) => {
                failures.push((fixture.name.clone(), e.to_string()));
            }
        }
    }
    assert!(failures.is_empty(), "plan fixture(s) failed: {failures:?}");
}

/// to print actual rand(42)/randn(42) values for tests/fixtures/with_rand_seed.json.
#[test]
#[ignore]
fn print_rand_seed_42_values() {
    use rand::Rng;
    use rand::SeedableRng;
    use rand_distr::Distribution;
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let dist = rand_distr::StandardNormal;
    println!("rand(42) 3 values:");
    for _ in 0..3 {
        println!("  {}", rng.gen::<f64>());
    }
    let mut rng2 = rand::rngs::StdRng::seed_from_u64(42);
    println!("randn(42) 3 values:");
    for _ in 0..3 {
        let v: f64 = dist.sample(&mut rng2);
        println!("  {v}");
    }
}
