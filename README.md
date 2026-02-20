# Robin Sparkless

**PySpark-style DataFrames in Rust—no JVM.** A DataFrame library that mirrors PySpark’s API and semantics while using [Polars](https://www.pola.rs/) as the execution engine.

[![CI](https://github.com/eddiethedean/robin-sparkless/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/robin-sparkless/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/robin-sparkless.svg)](https://crates.io/crates/robin-sparkless)
[![docs.rs](https://docs.rs/robin-sparkless/badge.svg)](https://docs.rs/robin-sparkless)
[![Documentation](https://readthedocs.org/projects/robin-sparkless/badge/?version=latest)](https://robin-sparkless.readthedocs.io/en/latest/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## Why Robin Sparkless?

- **Familiar API** — `SparkSession`, `DataFrame`, `Column`, and PySpark-like functions so you can reuse patterns without the JVM.
- **Polars under the hood** — Fast, native Rust execution with Polars for IO, expressions, and aggregations.
- **Persistence options** — Global temp views (cross-session in-memory) and disk-backed `saveAsTable` via `spark.sql.warehouse.dir`.
- **Sparkless backend target** — Designed to power [Sparkless](https://github.com/eddiethedean/sparkless) (the Python PySpark replacement) as a Rust execution engine.

---

## Features

| Area | What’s included |
|------|------------------|
| **Core** | `SparkSession`, `DataFrame`; lazy by default. **Two expression APIs:** (1) **ExprIr** (engine-agnostic): `col`, `lit_i64`, `gt`, `when`, … from crate root → `filter_expr_ir`, `select_expr_ir`, `collect_rows`, `agg_expr_ir`; (2) **Column/Expr** (Polars): `prelude` or `functions` → `filter`, `with_column`, `select_exprs`. Plus `order_by`, `group_by`, joins |
| **IO** | CSV, Parquet, JSON via `SparkSession::read_*` |
| **Expressions** | `col()`, `lit()`, `when`/`then`/`otherwise`, `coalesce`, cast, type/conditional helpers |
| **Aggregates** | `count`, `sum`, `avg`, `min`, `max`, and more; multi-column groupBy |
| **Window** | `row_number`, `rank`, `dense_rank`, `lag`, `lead`, `first_value`, `last_value`, and others with `.over()` |
| **Arrays & maps** | `array_*`, `explode`, `create_map`, `map_keys`, `map_values`, and related functions |
| **Strings & JSON** | String functions (`upper`, `lower`, `substring`, `regexp_*`, etc.), `get_json_object`, `from_json`, `to_json` |
| **Datetime & math** | Date/time extractors and arithmetic, `year`/`month`/`day`, math (`sin`, `cos`, `sqrt`, `pow`, …) |
| **Optional SQL** | `spark.sql("SELECT ...")` with temp views, global temp views (cross-session), and tables: `createOrReplaceTempView`, `createOrReplaceGlobalTempView`, `table(name)`, `table("global_temp.name")`, `df.write().saveAsTable(name, mode=...)`, `spark.catalog().listTables()` — enable with `--features sql` |
| **Optional Delta** | `read_delta(path)` or `read_delta(table_name)`, `read_delta_with_version`, `write_delta`, `write_delta_table(name)` — enable with `--features delta` (path I/O); table-by-name works with `sql` only |
| **UDFs** | Pure-Rust UDFs registered in a session-scoped registry; see `docs/UDF_GUIDE.md` |

**Parity:** 200+ fixtures validated against PySpark. Known differences from PySpark are documented in [docs/PYSPARK_DIFFERENCES.md](docs/PYSPARK_DIFFERENCES.md). Out-of-scope items (XML, UDTF, streaming, RDD) are in [docs/DEFERRED_SCOPE.md](docs/DEFERRED_SCOPE.md). Full parity status: [docs/PARITY_STATUS.md](docs/PARITY_STATUS.md).

---

## Installation

### Rust

Add to your `Cargo.toml`:

```toml
[dependencies]
robin-sparkless = "0.14.0"
```

Optional features:

```toml
robin-sparkless = { version = "0.14.0", features = ["sql"] }   # spark.sql(), temp views
robin-sparkless = { version = "0.14.0", features = ["delta"] }  # Delta Lake read/write
```

## Quick start

### Rust

**Engine-agnostic (ExprIr) API** — recommended for new code and embeddings; uses only `EngineError` and robin-sparkless types:

```rust
use robin_sparkless::{col, lit_i64, gt, SparkSession};

fn main() -> Result<(), robin_sparkless::EngineError> {
    let spark = SparkSession::builder().app_name("demo").get_or_create();
    let df = spark.create_dataframe_engine(
        vec![
            (1, 25, "Alice".to_string()),
            (2, 30, "Bob".to_string()),
            (3, 35, "Charlie".to_string()),
        ],
        vec!["id", "age", "name"],
    )?;
    let adults = df.filter_expr_ir(&gt(col("age"), lit_i64(26)))?;
    adults.show(Some(10)).map_err(robin_sparkless::to_engine_error)?;
    Ok(())
}
```

**Column (Polars) API** — full PySpark-like API with `Column` and `Expr`:

```rust
use robin_sparkless::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark = SparkSession::builder().app_name("demo").get_or_create();
    let df = spark.create_dataframe(
        vec![
            (1, 25, "Alice".to_string()),
            (2, 30, "Bob".to_string()),
            (3, 35, "Charlie".to_string()),
        ],
        vec!["id", "age", "name"],
    )?;
    let adults = df.filter(col("age").gt(lit_i64(26).into_expr()).into_expr())?;
    adults.show(Some(10))?;
    Ok(())
}
```

Output (from `show`; run with `cargo run --example demo`):

```
shape: (2, 3)
┌─────┬─────┬─────────┐
│ id  ┆ age ┆ name    │
│ --- ┆ --- ┆ ---     │
│ i64 ┆ i64 ┆ str     │
╞═════╪═════╪═════════╡
│ 2   ┆ 30  ┆ Bob     │
│ 3   ┆ 35  ┆ Charlie │
└─────┴─────┴─────────┘
```

You can also wrap an existing Polars `DataFrame` with `DataFrame::from_polars(polars_df)`. See [docs/QUICKSTART.md](docs/QUICKSTART.md) for joins, window functions, and more.

### Embedding robin-sparkless in your app

Use the **engine-agnostic ExprIr API** and `*_engine()` methods so your public surface does not depend on Polars types. Use [prelude](https://docs.rs/robin-sparkless/latest/robin_sparkless/prelude/index.html) for the full Column API, or root imports for ExprIr; optional [config from environment](https://docs.rs/robin-sparkless/latest/robin_sparkless/struct.SparklessConfig.html) for session setup. Results can be returned as JSON for bindings or CLI tools.

```toml
[dependencies]
robin-sparkless = "0.14.0"
```

```rust
use robin_sparkless::{col, lit_i64, gt, SparkSession, SparklessConfig, to_engine_error};

fn main() -> Result<(), robin_sparkless::EngineError> {
    let config = SparklessConfig::from_env();
    let spark = SparkSession::from_config(&config);

    let df = spark.create_dataframe_engine(
        vec![
            (1i64, 10i64, "a".to_string()),
            (2i64, 20i64, "b".to_string()),
            (3i64, 30i64, "c".to_string()),
        ],
        vec!["id", "value", "label"],
    )?;
    let filtered = df.filter_expr_ir(&gt(col("id"), lit_i64(1)))?;
    let json = filtered.to_json_rows()?;
    println!("{}", json);
    Ok(())
}
```

Example output (from the snippet above or `cargo run --example embed_readme`; JSON key order may vary):

```
[{"id":2,"value":20,"label":"b"},{"id":3,"value":30,"label":"c"}]
```

Run the [embed_basic](examples/embed_basic.rs) example: `cargo run --example embed_basic`. For a minimal FFI surface, use `robin_sparkless::prelude::embed` and the ExprIr API: `create_dataframe_engine`, `filter_expr_ir`, `select_expr_ir`, `collect_rows`, `agg_expr_ir`, plus schema helpers (`StructType::to_json`, `schema_from_json`). Convert Polars errors with `to_engine_error`. See [docs/EMBEDDING.md](docs/EMBEDDING.md).

## Development

**Prerequisites:** Rust (see [rust-toolchain.toml](rust-toolchain.toml)).

This repository is a **Cargo workspace**. The main library is **robin-sparkless** (the facade); most users depend only on it. The workspace also includes **robin-sparkless-core** (engine-agnostic types, expression IR, config, error; no Polars) and **robin-sparkless-polars** (Polars backend: Column, functions, UDFs). These are publishable for advanced or minimal-use cases. `make check` and CI build the whole workspace.

| Command | Description |
|---------|-------------|
| `cargo build` | Build (Rust only) |
| `cargo build --workspace --all-features` | Build all workspace crates with optional features |
| `cargo test` | Run Rust tests |
| `make test` | Run Rust tests (wrapper for `cargo test --workspace`) |
| `make check` | Rust only: format check, clippy, audit, deny, Rust tests. Use `make -j5 check` to run the five jobs in parallel. |
| `make check-full` | Full Rust check suite (what CI runs): `fmt --check`, clippy, audit, deny, tests. |
| `make clean` | Remove `target/` (e.g. to free disk without running check; `check-full` already cleans before each run so binaries don't accumulate). |
| `make fmt` | Format Rust code (run before check if you want to fix formatting). |
| `make test-parity-phase-a` … `make test-parity-phase-g` | Run parity fixtures for a specific phase (see [PARITY_STATUS](docs/PARITY_STATUS.md)). |
| `make test-parity-phases` | Run all parity phases (A–G) via the parity harness. |
| `make sparkless-parity` | When `SPARKLESS_EXPECTED_OUTPUTS` is set and PySpark/Java are available, convert Sparkless fixtures, regenerate expected from PySpark, and run Rust parity tests. |
| `cargo bench` | Benchmarks (robin-sparkless vs Polars) |
| `cargo doc --open` | Build and open API docs |

CI runs format, clippy, audit, deny, Rust tests, and parity tests on push/PR (see [.github/workflows/ci.yml](.github/workflows/ci.yml)).

---

## Documentation

| Resource | Description |
|----------|-------------|
| [**Read the Docs**](https://robin-sparkless.readthedocs.io/) | Full docs: quickstart, Rust usage, Sparkless integration (MkDocs) |
| [**docs.rs**](https://docs.rs/robin-sparkless) | Rust API reference |
| [QUICKSTART](docs/QUICKSTART.md) | Build, usage, optional features, benchmarks |
| [User Guide](docs/USER_GUIDE.md) | Everyday usage (Rust) |
| [Persistence Guide](docs/PERSISTENCE_GUIDE.md) | Global temp views, disk-backed saveAsTable |
| [UDF Guide](docs/UDF_GUIDE.md) | Scalar, vectorized, and grouped UDFs |
| [PySpark Differences](docs/PYSPARK_DIFFERENCES.md) | Known divergences |
| [Roadmap](docs/ROADMAP.md) | Development phases, Sparkless integration |
| [RELEASING](docs/RELEASING.md) | Publishing to crates.io |

See [CHANGELOG.md](CHANGELOG.md) for version history.

---

## License

MIT
