use std::fs;
use std::path::Path;

use polars::prelude::{
    DataFrame as PlDataFrame, Expr, NamedFrom, PolarsError, Series, col, lit,
};
use robin_sparkless::DataFrame;
use serde::Deserialize;
use serde_json::Value;

/// Top-level fixture structure, matching the JSON we’ll generate from PySpark.
#[derive(Debug, Deserialize)]
struct Fixture {
    name: String,
    #[allow(dead_code)]
    pyspark_version: Option<String>,
    input: InputSection,
    operations: Vec<Operation>,
    expected: ExpectedSection,
}

#[derive(Debug, Deserialize)]
struct InputSection {
    schema: Vec<ColumnSpec>,
    rows: Vec<Vec<Value>>,
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
    OrderBy { columns: Vec<String>, #[serde(default)] ascending: Vec<bool> },
}

/// Parity tests generated from PySpark fixtures.
///
/// NOTE: This test is `ignored` by default until we have:
/// - A stable fixture format and generator script.
/// - A first slice of Rust API capable of running the described operations.
#[test]
#[ignore]
fn pyspark_parity_fixtures() {
    let fixtures_dir = Path::new("tests/fixtures");
    if !fixtures_dir.exists() {
        // Nothing to run yet; treat as a no-op.
        return;
    }

    for entry in fs::read_dir(fixtures_dir).expect("read fixtures directory") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }

        let text = fs::read_to_string(&path).expect("read fixture");
        let fixture: Fixture =
            serde_json::from_str(&text).expect("parse fixture json");

        // For now, we just ensure we can:
        // - Reconstruct a DataFrame from the input section, and
        // - Apply the basic operations (filter + select + orderBy) without panicking.
        run_fixture_smoke(&fixture).unwrap();
    }
}

fn run_fixture_smoke(fixture: &Fixture) -> Result<(), PolarsError> {
    // Basic shape sanity.
    assert!(
        !fixture.input.schema.is_empty(),
        "fixture {} has empty schema",
        fixture.name
    );
    assert_eq!(
        fixture.expected.schema.len(),
        fixture
            .expected
            .rows
            .first()
            .map(|r| r.len())
            .unwrap_or(0),
        "fixture {} expected schema/row length mismatch",
        fixture.name
    );

    // Reconstruct a DataFrame from the input section.
    let df = create_df_from_input(&fixture.input)?;

    // Apply the basic operations; we ignore the final result for now.
    let _ = apply_operations(df, &fixture.operations)?;

    Ok(())
}

/// Build a Polars-backed `DataFrame` from the JSON input section.
///
/// For the first parity slice we support only a small subset of types:
/// - `int` / `bigint` → `i64`
/// - `string`         → UTF-8
fn create_df_from_input(input: &InputSection) -> Result<DataFrame, PolarsError> {
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
            "string" | "str" | "varchar" => {
                let mut vals: Vec<Option<String>> =
                    Vec::with_capacity(input.rows.len());
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

/// Apply the first parity-slice operations (filter + select + orderBy).
///
/// - `filter` supports very simple expressions of the form:
///   - `col('age') > 30`
///   - `col(\"age\") >= 10`
/// - `select` takes explicit column names.
/// - `orderBy` is currently a no-op here; we will handle ordering in the
///   comparison phase once we materialize full parity tests.
fn apply_operations(
    mut df: DataFrame,
    ops: &[Operation],
) -> Result<DataFrame, PolarsError> {
    for op in ops {
        match op {
            Operation::Filter { expr } => {
                let predicate = parse_simple_filter_expr(expr).map_err(|e| {
                    PolarsError::ComputeError(
                        format!("failed to parse filter expr '{}': {}", expr, e).into(),
                    )
                })?;
                df = df.filter(predicate)?;
            }
            Operation::Select { columns } => {
                let cols: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                df = df.select(cols)?;
            }
            Operation::OrderBy { .. } => {
                // TODO: once we have a stable sorting API on `DataFrame`,
                // either call it here or sort at comparison time.
            }
        }
    }
    Ok(df)
}

/// Very small parser for expressions like: `col('age') > 30`.
fn parse_simple_filter_expr(src: &str) -> Result<Expr, String> {
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

    let col_name = &s[quote1 + 1..quote2];

    // After the closing paren, expect an operator and a literal integer.
    let after_col = &s[quote2 + 1..];
    let tokens: Vec<&str> = after_col.split_whitespace().collect();
    if tokens.len() < 2 {
        return Err("expected operator and literal".to_string());
    }

    let op = tokens[0];
    let lit_str = tokens[1];
    let lit_val: i64 = lit_str
        .parse()
        .map_err(|_| format!("unable to parse literal '{}'", lit_str))?;

    let c = col(col_name);
    let lit_e = lit(lit_val);

    let expr = match op {
        ">" => c.gt(lit_e),
        ">=" => c.gt_eq(lit_e),
        "<" => c.lt(lit_e),
        "<=" => c.lt_eq(lit_e),
        "==" | "=" => c.eq(lit_e),
        "!=" => c.neq(lit_e),
        other => return Err(format!("unsupported operator '{}'", other)),
    };

    Ok(expr)
}

