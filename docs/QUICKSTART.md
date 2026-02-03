# Quick Start Guide

Robin-sparkless is a PySpark-like DataFrame library in Rust, with Polars as the engine. Long-term, it is designed to replace the backend of [Sparkless](https://github.com/eddiethedean/sparkless). See [SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) for integration details.

## Building the Project

### Prerequisites
- Rust (latest stable)

### Build (Rust only)

```bash
cargo build
```

### Build with Python extension (optional)

```bash
pip install maturin
maturin develop --features pyo3   # Editable install into current env
# With optional SQL and/or Delta:
maturin develop --features "pyo3,sql"       # spark.sql(), temp views
maturin develop --features "pyo3,delta"      # read_delta, write_delta
maturin develop --features "pyo3,sql,delta" # All optional features
```

Then use the `robin_sparkless` Python module; see [PYTHON_API.md](PYTHON_API.md).

### Add as a dependency

In your own crate:

```toml
[dependencies]
robin-sparkless = "0.1.0"
```

## Basic Usage

The current API is intentionally small and focused on wrapping Polars types.

```rust
use polars::prelude::*;
use robin_sparkless::{DataFrame, Column};

fn main() -> polars::prelude::PolarsResult<()> {
    // Build a Polars DataFrame
    let polars_df = df!(
        "id" => &[1, 2, 3],
        "age" => &[25, 30, 35],
        "name" => &["Alice", "Bob", "Charlie"],
    )?;

    // Wrap it
    let df = DataFrame::from_polars(polars_df);

    // Introspect
    println!("{:?}", df.columns()?);

    // Use a Column wrapper around a Polars expression
    let age_col: Column = df.column("age")?;
    let adults = df.filter(age_col.expr().clone().gt(lit(18)))?;

    adults.show(Some(10))?;

    Ok(())
}
```

### Joins

```rust
use robin_sparkless::{DataFrame, JoinType};

let joined = left_df.join(&right_df, vec!["dept_id"], JoinType::Inner)?;
```

Supported join types: `Inner`, `Left`, `Right`, `Outer`.

### Window Functions

```rust
use robin_sparkless::{col, DataFrame};

// row_number, rank, dense_rank over partition
let df_with_rn = df.with_column(
    "rn",
    col("salary").row_number(true).over(&["dept"]).into_expr(),
)?;

// lag and lead
let df_with_lag = df.with_column(
    "prev_salary",
    col("salary").lag(1).over(&["dept"]).into_expr(),
)?;
```

Supported window functions: `row_number()`, `rank()`, `dense_rank()`, `lag(n)`, `lead(n)` — each used with `.over(&["col1", "col2"])` for partitioning.

### String Functions

```rust
use robin_sparkless::{col, concat, concat_ws, DataFrame};

// upper, lower, substring (1-based start) - via Column methods
let df = df.with_column("name_upper", col("name").upper().into_expr())?;
let df = df.with_column("prefix", col("name").substr(1, Some(3)).into_expr())?;

// concat and concat_ws
let df = df.with_column("full", concat(&[&col("first"), &col("last")]).into_expr())?;
let df = df.with_column("joined", concat_ws("-", &[&col("a"), &col("b")]).into_expr())?;
```

Additional string: `length`, `trim`, `ltrim`, `rtrim`, `regexp_extract`, `regexp_replace`, `regexp_extract_all`, `regexp_like`, `split`, `initcap`, `mask`, `translate`, `substring_index` (Phase 10); **Phase 16**: `regexp_count`, `regexp_instr`, `regexp_substr`, `split_part`, `find_in_set`, `format_string`, `printf`. DataFrame methods (Phase 12): `sample`, `random_split`, `first`, `head`, `tail`, `take`, `is_empty`, `to_json`, `to_pandas`, `explain`, `print_schema`, `summary`, `to_df`, `select_expr`, `col_regex`, `with_columns`, `with_columns_renamed`, `stat()` (cov/corr), `na()` (fill/drop), `freq_items`, `approx_quantile`, `crosstab`, `melt`, `sample_by`, etc. See [PYTHON_API.md](PYTHON_API.md) for the full Python API.

### Datetime

For date/datetime columns: `year()`, `month()`, `day()`, `to_date()` (cast to date), `date_format(format)` (chrono strftime, e.g. `"%Y-%m-%d"`), `hour()`, `minute()`, `second()`, `date_add(n)`, `date_sub(n)`, `datediff(end, start)`, `last_day()`, `trunc(format)`. **Phase 14**: `quarter()`, `weekofyear()`/`week()`, `dayofweek()` (1=Sun..7=Sat), `dayofyear()`, `add_months(n)`, `months_between(end, start)`, `next_day(day_of_week)` (e.g. `"Mon"`, `"Tue"`). **Phase 17**: `unix_timestamp()`, `unix_timestamp(col, format)`, `from_unixtime(col, format)`, `make_date(year, month, day)`, `timestamp_seconds(col)`, `timestamp_millis(col)`, `timestamp_micros(col)`, `unix_date(col)`, `date_from_unix_date(col)`.

### Math and type (Phase 14)

Math (radians): `sin()`, `cos()`, `tan()`, `asin()`, `acos()`, `atan()`, `atan2(y, x)`, `degrees()`, `radians()`, `signum()` (plus existing `sqrt`, `pow`, `exp`, `log`). **Phase 17**: `pmod(dividend, divisor)`, `factorial(n)`. Type: `cast(column, type_name)` (strict; use type names like `"int"`, `"long"`, `"double"`, `"string"`, `"date"`, `"timestamp"`), `try_cast(column, type_name)` (null on failure), `isnan(column)`; conditional: `greatest(columns...)`, `least(columns...)`.

### Parity

Behavior is validated against PySpark on **103 parity fixtures** (~192+ functions); see [PARITY_STATUS.md](PARITY_STATUS.md). Known differences are in [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md). CI (GitHub Actions) runs format, clippy, audit, deny, and all tests (including parity) on every push/PR.

For roadmap and Sparkless integration phases (Phases 12–17 completed; Phases 18–19 remaining), see [ROADMAP.md](ROADMAP.md).

## Troubleshooting

### Common errors

- **Column 'X' not found** — The DataFrame has no column with that name (case-sensitive if `spark.sql.caseSensitive` is true). The error message lists available columns; check spelling and case.

- **create_dataframe: expected 3 column names** — `create_dataframe` accepts only `(i64, i64, String)` rows and exactly three column names. Use `["id", "age", "name"]` or similar.

- **Type coercion: cannot find common type** — A comparison or arithmetic involved incompatible types (e.g. string vs numeric). Cast one side with `.cast()` or use compatible types.

- **read_csv(path): ...** — The path may not exist, not be readable, or the file may not be valid CSV. Ensure the path is correct and the file has a header row if using default options.

### Optional features (Rust)

- **SQL** (`--features sql`): `SparkSession::sql(query)` with temp views (`create_or_replace_temp_view`, `table`). Supports single SELECT, FROM/JOIN, WHERE, GROUP BY, ORDER BY, LIMIT.
- **Delta Lake** (`--features delta`): `read_delta(path)`, `read_delta_with_version(path, version)` (time travel), `write_delta(path, overwrite)` on DataFrame. Requires `deltalake` + tokio.

### Benchmarks

Run `cargo bench` to compare robin-sparkless vs plain Polars on filter → select → groupBy pipelines. The goal is to stay within about 2x of Polars for supported operations.
