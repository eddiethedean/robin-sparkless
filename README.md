# Robin Sparkless

A Rust DataFrame library that aims to **emulate PySpark’s DataFrame behavior and semantics without requiring the JVM**, using Polars as the execution engine under the hood.

**Long-term goal**: Robin-sparkless is designed to **replace the backend logic** of [Sparkless](https://github.com/eddiethedean/sparkless)—the Python PySpark drop-in replacement—so Sparkless can call robin-sparkless via PyO3/FFI for DataFrame execution. See [docs/SPARKLESS_INTEGRATION_ANALYSIS.md](docs/SPARKLESS_INTEGRATION_ANALYSIS.md) for architecture, structural learnings, and test conversion strategy.

## Features

- **PySpark-inspired API**: `SparkSession`, `DataFrame`, `Column` modeled after PySpark concepts
- **PySpark-like behavior**: Semantics (null handling, groupBy, joins, etc.) should match PySpark as closely as practical
- **Polars-backed**: Uses Polars `DataFrame`/`Expr` internally for performance
- **Pure Rust**: Default build has no Python runtime; optional **PyO3** feature for Python bindings
- **Sparkless backend target**: Intended to power Sparkless's execution engine; aligns with its 403+ PySpark functions and 270+ test fixtures

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

The library supports IO (CSV/Parquet/JSON via `SparkSession::read_*`), joins (`DataFrame::join` with Inner/Left/Right/Outer), groupBy aggregates (including multi-agg), window functions (`row_number`, `rank`, `dense_rank`, `lag`, `lead` with `.over()`), and string functions (`upper`, `lower`, `substring`, `concat`, `concat_ws`). See [docs/QUICKSTART.md](docs/QUICKSTART.md) for more examples.

## Development

### Prerequisites

- Rust (latest stable)

### Building

```bash
# Build the project (Rust only)
cargo build

# Build with Python extension (optional)
cargo build --features pyo3

# Run tests (Rust only; PyO3 code is behind the feature)
cargo test

# Run all tests (Rust + Python; creates .venv and installs extension for Python tests)
make test

# Run all checks (format, clippy, audit, deny, tests)
make check
# Or: RUSTUP_TOOLCHAIN=stable make check

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

- **SparkSession**: Entry point for creating `DataFrame`s and reading data sources (CSV, Parquet, JSON).
- **DataFrame**: Main tabular data structure; operations include `filter`, `select`, `order_by`, `group_by`, `with_column`, `join`.
- **Column**: Represents expressions over columns, similar to PySpark’s `Column`; includes window methods (`rank`, `row_number`, `dense_rank`, `lag`, `lead`) with `.over()`.
- **Functions**: Helper functions like `col()`, `lit_*()`, `count()`, `when`, `coalesce`, window functions, etc., modeled after PySpark’s `pyspark.sql.functions`.

Core behavior (null handling, grouping semantics, joins, window functions, string functions, expression behavior) matches PySpark on 51 parity fixtures, with more coverage planned.

## Related Documentation

- [docs/](docs/README.md) – Documentation index
- [CHANGELOG.md](CHANGELOG.md) – Version history and release notes
- [docs/SPARKLESS_INTEGRATION_ANALYSIS.md](docs/SPARKLESS_INTEGRATION_ANALYSIS.md) – Sparkless backend replacement strategy, architecture learnings, test conversion
- [docs/ROADMAP.md](docs/ROADMAP.md) – Development roadmap including Sparkless integration phases
- [docs/FULL_BACKEND_ROADMAP.md](docs/FULL_BACKEND_ROADMAP.md) – Phased plan to full Sparkless backend replacement (400+ functions, PyO3 bridge)
- [docs/PYTHON_API.md](docs/PYTHON_API.md) – Python API contract (Phase 4 PyO3 bridge): build, install, method signatures, data transfer
- [docs/PARITY_STATUS.md](docs/PARITY_STATUS.md) – PySpark parity coverage matrix (51 fixtures)

## License

MIT
