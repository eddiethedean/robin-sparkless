# Robin Sparkless

A Rust DataFrame library that aims to **emulate PySpark’s DataFrame behavior and semantics without requiring the JVM**, using Polars as the execution engine under the hood.

**Long-term goal**: Robin-sparkless is designed to **replace the backend logic** of [Sparkless](https://github.com/eddiethedean/sparkless)—the Python PySpark drop-in replacement—so Sparkless can call robin-sparkless via PyO3/FFI for DataFrame execution. See [docs/SPARKLESS_INTEGRATION_ANALYSIS.md](docs/SPARKLESS_INTEGRATION_ANALYSIS.md) for architecture, structural learnings, and test conversion strategy.

## Features

- **PySpark-inspired API**: `SparkSession`, `DataFrame`, `Column` modeled after PySpark concepts
- **PySpark-like behavior**: Semantics (null handling, groupBy, joins, etc.) should match PySpark as closely as practical
- **Polars-backed**: Uses Polars `DataFrame`/`Expr` internally for performance
- **Pure Rust**: Default build has no Python runtime; optional **PyO3** feature for Python bindings
- **Optional SQL** (`--features sql`): `spark.sql("SELECT ...")` with temp views (`createOrReplaceTempView`, `table`); single SELECT, FROM/JOIN, WHERE, GROUP BY, ORDER BY, LIMIT
- **Optional Delta Lake** (`--features delta`): `read_delta` / `read_delta_with_version` (time travel), `write_delta` (overwrite/append) via delta-rs
- **Benchmarks**: `cargo bench` compares robin-sparkless vs plain Polars; target within ~2x for supported pipelines
- **Sparkless backend target**: Intended to power Sparkless's execution engine; aligns with its 403+ PySpark functions and 270+ test fixtures; **82 parity fixtures** passing (~130+ functions; Phase 12: DataFrame methods ~55+; Phase 13: string/binary/collection batch — ascii, format_number, overlay, position, char, chr, base64, unbase64, sha1, sha2, md5, array_compact). Path to 100%: ROADMAP Phases 14–17 (functions → 403, fixtures 150+, Phase 16: publish crate, then Sparkless integration).

## Installation

### Rust

Add to your `Cargo.toml`:

```toml
[dependencies]
robin-sparkless = "0.1.0"
```

### Python (PyO3 bridge, optional)

Build the Python extension with [maturin](https://www.maturin.rs/) (requires Rust and Python 3.8+):

```bash
# From the repo root
pip install maturin
maturin develop --features pyo3   # Editable install
# or build a wheel:
maturin build --features pyo3
pip install target/wheels/robin_sparkless-*.whl
```

Then use the `robin_sparkless` module (see [docs/PYTHON_API.md](docs/PYTHON_API.md) for the full API contract):

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("test").get_or_create()
df = spark.create_dataframe([(1, 25, "Alice"), (2, 30, "Bob")], ["id", "age", "name"])
filtered = df.filter(rs.col("age").gt(rs.lit(26)))
print(filtered.collect())  # [{"id": 2, "age": 30, "name": "Bob"}]
```

## Quick Start

At the moment, `robin-sparkless` exposes a small Rust API and uses Polars as its execution engine. The long-term goal is to make PySpark-style code patterns natural to express in Rust.

```rust
use polars::prelude::*;
use robin_sparkless::{DataFrame, Column, SparkSession};

// In the future, most users should start from a SparkSession, like PySpark:
// let spark = SparkSession::builder().app_name("my_app").get_or_create();
// let df = spark.create_dataframe(data, schema);
//
// For now, we start from a Polars DataFrame:

let polars_df = df!(
    "id" => &[1, 2, 3, 4],
    "age" => &[25, 35, 45, 55],
    "score" => &[50, 60, 110, 70],
)?;

let df = DataFrame::from_polars(polars_df);

// Introspect (PySpark-style: df.schema, df.columns)
let schema = df.schema()?;
let columns = df.columns()?;

// Build expressions using a Column wrapper (similar to PySpark Column)
let age_col: Column = df.column("age")?;
let adults = df.filter(age_col.expr().clone().gt(lit(30)))?;

// More complex boolean logic, similar to PySpark:
// df.filter((col("age") > 30) & ((col("score") < 100) | (col("age") > 50)))
let complex = df.filter(
    (df.column("age")?.expr().clone().gt(lit(30)))
        .and(
            df.column("score")?.expr().clone().lt(lit(100))
                .or(df.column("age")?.expr().clone().gt(lit(50))),
        ),
)?;

// Complex expressions in withColumn (arithmetic and logical):
use robin_sparkless::{col, lit_i64};
let df_with_computed = df.with_column(
    "above_threshold",
    (col("age").into_expr() + col("score").into_expr()).gt(lit(100))
)?;

adults.show(Some(10))?;
complex.show(Some(10))?;
df_with_computed.show(Some(10))?;
```

The library supports IO (CSV/Parquet/JSON via `SparkSession::read_*`), joins (`DataFrame::join` with Inner/Left/Right/Outer), groupBy aggregates (including multi-agg), window functions (`row_number`, `rank`, `dense_rank`, `lag`, `lead`, `first_value`, `last_value`, `percent_rank`, `cume_dist`, `ntile`, `nth_value` with `.over()`), array functions (`array_size`, `array_contains`, `element_at`, `explode`, `array_sort`, `array_join`, `array_slice`, `array_position`, `array_remove`, `posexplode`, `array_exists`, `array_forall`, `array_filter`, `array_transform`, `array_sum`, `array_mean`, `array_repeat`, `array_flatten`, `array_compact`), string functions (`upper`, `lower`, `substring`, `concat`, `concat_ws`, `length`, `trim`, `regexp_extract`, `regexp_replace`, `regexp_extract_all`, `regexp_like`, `split`, `mask`, `translate`, `substring_index`, `soundex`, `levenshtein`, `crc32`, `xxhash64`; Phase 13: `ascii`, `format_number`, `overlay`, `position`, `char`, `chr`, `base64`, `unbase64`, `sha1`, `sha2`, `md5`), JSON (`get_json_object`, `from_json`, `to_json`), Map (`create_map`, `map_keys`, `map_values`, `map_entries`, `map_from_arrays`), and datetime helpers (`year`, `month`, `day`, `to_date`, `date_format`, `current_date`, `date_add`, etc.). See [docs/QUICKSTART.md](docs/QUICKSTART.md) for more examples.

## Development

### Prerequisites

- Rust (latest stable)

### Building

```bash
# Build the project (Rust only)
cargo build

# Build with Python extension (optional)
cargo build --features pyo3

# Optional: SQL support and/or Delta Lake
cargo build --features "pyo3,sql"       # SQL: spark.sql(), temp views
cargo build --features "pyo3,delta"      # Delta: read_delta, write_delta
cargo build --features "pyo3,sql,delta"  # All optional features

# Run tests (Rust only; PyO3 code is behind the feature)
cargo test

# Run all tests (Rust + Python; creates .venv and installs extension for Python tests)
make test

# Run all checks (format, clippy, audit, deny, tests)
make check
# Or: RUSTUP_TOOLCHAIN=stable make check
# CI (GitHub Actions) runs the same checks on push/PR; see .github/workflows/ci.yml.

# Benchmarks (robin-sparkless vs Polars)
cargo bench

# Sparkless parity (optional: convert from Sparkless expected_outputs, then run parity)
# export SPARKLESS_EXPECTED_OUTPUTS=/path/to/sparkless/tests/expected_outputs
# make sparkless-parity

# Build documentation
cargo doc --open
```

### Python tests

```bash
make test        # Runs Rust tests, then Python tests (creates .venv, maturin develop --features pyo3, pytest)
make test-python # Python tests only
```

Or manually (with virtualenv activated): `maturin develop --features pyo3` then `pytest tests/python/`.

## Architecture

Robin Sparkless aims to provide a **PySpark-like API layer** on top of Polars:

- **SparkSession**: Entry point for creating `DataFrame`s and reading data sources (CSV, Parquet, JSON); optional **SQL** (`sql()`, temp views) and **Delta** (`read_delta`, `read_delta_with_version`) when features are enabled.
- **DataFrame**: Main tabular data structure; operations include `filter`, `select`, `order_by`, `group_by`, `with_column`, `join`; optional `write_delta` when the `delta` feature is enabled.
- **Column**: Represents expressions over columns, similar to PySpark’s `Column`; includes window methods (`rank`, `row_number`, `dense_rank`, `lag`, `lead`) with `.over()`.
- **Functions**: Helper functions like `col()`, `lit_*()`, `count()`, `when`, `coalesce`, window functions, etc., modeled after PySpark’s `pyspark.sql.functions`.

Core behavior (null handling, grouping semantics, joins, window functions, array and string functions, JSON, Map, expression behavior) matches PySpark on 82 parity fixtures (~130+ functions). Phase 12 completed DataFrame methods parity: sample, random_split, first/head/tail/take, is_empty, to_json, explain, print_schema, checkpoint, repartition, coalesce, offset, summary, stat (cov/corr), to_df, select_expr, col_regex, with_columns, with_columns_renamed, na (fill/drop), to_pandas, freq_items, approx_quantile, crosstab, melt, except_all, intersect_all, sample_by, and Spark no-ops (hint, is_local, input_files, etc.). Python bindings expose all of these via the `robin_sparkless` module; see [docs/PYTHON_API.md](docs/PYTHON_API.md). Known divergences are documented in [docs/PYSPARK_DIFFERENCES.md](docs/PYSPARK_DIFFERENCES.md).

## Related Documentation

- [docs/](docs/README.md) – Documentation index
- [CHANGELOG.md](CHANGELOG.md) – Version history and release notes
- [docs/SPARKLESS_INTEGRATION_ANALYSIS.md](docs/SPARKLESS_INTEGRATION_ANALYSIS.md) – Sparkless backend replacement strategy, architecture learnings, test conversion
- [docs/ROADMAP.md](docs/ROADMAP.md) – Development roadmap; Phase 12 completed; Phases 13–17 (functions, crate publish, then Sparkless integration)
- [docs/FULL_BACKEND_ROADMAP.md](docs/FULL_BACKEND_ROADMAP.md) – Phased plan to full Sparkless backend replacement (400+ functions, PyO3 bridge)
- [docs/PYTHON_API.md](docs/PYTHON_API.md) – Python API contract (Phase 4 PyO3 bridge): build, install, method signatures, data transfer
- [docs/PARITY_STATUS.md](docs/PARITY_STATUS.md) – PySpark parity coverage matrix (82 fixtures)
- [docs/PYSPARK_DIFFERENCES.md](docs/PYSPARK_DIFFERENCES.md) – Known divergences from PySpark (window, SQL, Delta; Phase 8 completed)
- [docs/CONVERTER_STATUS.md](docs/CONVERTER_STATUS.md) – Sparkless → robin-sparkless fixture converter and operation mapping
- [docs/SPARKLESS_PARITY_STATUS.md](docs/SPARKLESS_PARITY_STATUS.md) – Phase 5: pass/fail and failure reasons for converted fixtures; `make sparkless-parity`

## License

MIT
