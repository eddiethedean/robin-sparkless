# Robin Sparkless

**PySpark-style DataFrames in Rust—no JVM.** A DataFrame library that mirrors PySpark’s API and semantics while using [Polars](https://www.pola.rs/) as the execution engine.

[![crates.io](https://img.shields.io/crates/v/robin-sparkless.svg)](https://crates.io/crates/robin-sparkless)
[![docs.rs](https://docs.rs/robin-sparkless/badge.svg)](https://docs.rs/robin-sparkless)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## Why Robin Sparkless?

- **Familiar API** — `SparkSession`, `DataFrame`, `Column`, and PySpark-like functions so you can reuse patterns without the JVM.
- **Polars under the hood** — Fast, native Rust execution with Polars for IO, expressions, and aggregations.
- **Rust-first, Python optional** — Use it as a Rust library or build the Python extension via PyO3 for a drop-in style API.
- **Sparkless backend target** — Designed to power [Sparkless](https://github.com/eddiethedean/sparkless) (the Python PySpark replacement) so Sparkless can run on this engine via PyO3.

---

## Features

| Area | What’s included |
|------|------------------|
| **Core** | `SparkSession`, `DataFrame`, `Column`; `filter`, `select`, `with_column`, `order_by`, `group_by`, joins |
| **IO** | CSV, Parquet, JSON via `SparkSession::read_*` |
| **Expressions** | `col()`, `lit()`, `when`/`then`/`otherwise`, `coalesce`, cast, type/conditional helpers |
| **Aggregates** | `count`, `sum`, `avg`, `min`, `max`, and more; multi-column groupBy |
| **Window** | `row_number`, `rank`, `dense_rank`, `lag`, `lead`, `first_value`, `last_value`, and others with `.over()` |
| **Arrays & maps** | `array_*`, `explode`, `create_map`, `map_keys`, `map_values`, and related functions |
| **Strings & JSON** | String functions (`upper`, `lower`, `substring`, `regexp_*`, etc.), `get_json_object`, `from_json`, `to_json` |
| **Datetime & math** | Date/time extractors and arithmetic, `year`/`month`/`day`, math (`sin`, `cos`, `sqrt`, `pow`, …) |
| **Optional SQL** | `spark.sql("SELECT ...")` with temp views (`createOrReplaceTempView`, `table`) — enable with `--features sql` |
| **Optional Delta** | `read_delta`, `read_delta_with_version`, `write_delta` — enable with `--features delta` |

Known differences from PySpark are documented in [docs/PYSPARK_DIFFERENCES.md](docs/PYSPARK_DIFFERENCES.md). Parity status and roadmap are in [docs/PARITY_STATUS.md](docs/PARITY_STATUS.md) and [docs/ROADMAP.md](docs/ROADMAP.md).

---

## Installation

### Rust

Add to your `Cargo.toml`:

```toml
[dependencies]
robin-sparkless = "0.1.0"
```

Optional features:

```toml
robin-sparkless = { version = "0.1.0", features = ["sql"] }   # spark.sql(), temp views
robin-sparkless = { version = "0.1.0", features = ["delta"] }  # Delta Lake read/write
```

### Python (PyO3)

Build the Python extension with [maturin](https://www.maturin.rs/) (Rust + Python 3.8+):

```bash
pip install maturin
maturin develop --features pyo3
# With optional SQL and/or Delta:
maturin develop --features "pyo3,sql"
maturin develop --features "pyo3,delta"
maturin develop --features "pyo3,sql,delta"
```

Then use the `robin_sparkless` module; see [docs/PYTHON_API.md](docs/PYTHON_API.md).

---

## Quick start

### Rust

```rust
use robin_sparkless::{col, lit_i64, SparkSession};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark = SparkSession::builder().app_name("demo").get_or_create();

    // Create a DataFrame from rows (id, age, name)
    let df = spark.create_dataframe(
        vec![
            (1, 25, "Alice".to_string()),
            (2, 30, "Bob".to_string()),
            (3, 35, "Charlie".to_string()),
        ],
        vec!["id", "age", "name"],
    )?;

    // Filter and show
    let adults = df.filter(col("age").gt(lit_i64(26)))?;
    adults.show(Some(10))?;

    Ok(())
}
```

You can also wrap an existing Polars `DataFrame` with `DataFrame::from_polars(polars_df)`. See [docs/QUICKSTART.md](docs/QUICKSTART.md) for joins, window functions, and more.

### Python

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("demo").get_or_create()
df = spark.create_dataframe([(1, 25, "Alice"), (2, 30, "Bob")], ["id", "age", "name"])
filtered = df.filter(rs.col("age").gt(rs.lit(26)))
print(filtered.collect())  # [{"id": 2, "age": 30, "name": "Bob"}]
```

---

## Development

**Prerequisites:** Rust (see [rust-toolchain.toml](rust-toolchain.toml)), and for Python tests: Python 3.8+, `maturin`, `pytest`.

| Command | Description |
|---------|-------------|
| `cargo build` | Build (Rust only) |
| `cargo build --features pyo3` | Build with Python extension |
| `cargo test` | Run Rust tests |
| `make test` | Run Rust + Python tests (creates venv, `maturin develop`, `pytest`) |
| `make check` | Format, clippy, audit, deny, tests |
| `cargo bench` | Benchmarks (robin-sparkless vs Polars) |
| `cargo doc --open` | Build and open API docs |

CI runs the same checks on push/PR (see [.github/workflows/ci.yml](.github/workflows/ci.yml)).

---

## Documentation

- [**API reference (docs.rs)**](https://docs.rs/robin-sparkless) — Crate API
- [**docs/README.md**](docs/README.md) — Documentation index
- [**QUICKSTART.md**](docs/QUICKSTART.md) — Build, usage, optional features, benchmarks
- [**ROADMAP.md**](docs/ROADMAP.md) — Development roadmap and Sparkless integration
- [**PYSPARK_DIFFERENCES.md**](docs/PYSPARK_DIFFERENCES.md) — Known divergences from PySpark
- [**RELEASING.md**](docs/RELEASING.md) — Releasing and publishing to crates.io
- [**SPARKLESS_INTEGRATION_ANALYSIS.md**](docs/SPARKLESS_INTEGRATION_ANALYSIS.md) — Sparkless backend strategy and architecture

See also [CHANGELOG.md](CHANGELOG.md) for version history.

---

## License

MIT
