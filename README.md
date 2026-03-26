# Robin Sparkless

**PySpark-style DataFrames in Python—no JVM.** Install the **`sparkless`** package for a drop-in local PySpark replacement (fast unit tests + CI, no Java/Spark). Under the hood it’s powered by the `robin-sparkless` Rust engine using [Polars](https://www.pola.rs/) for execution.

[![CI](https://github.com/eddiethedean/robin-sparkless/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/robin-sparkless/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/sparkless.svg)](https://pypi.org/project/sparkless/)
[![crates.io](https://img.shields.io/crates/v/robin-sparkless.svg)](https://crates.io/crates/robin-sparkless)
[![docs.rs](https://docs.rs/robin-sparkless/badge.svg)](https://docs.rs/robin-sparkless)
[![Documentation](https://readthedocs.org/projects/robin-sparkless/badge/?version=latest)](https://robin-sparkless.readthedocs.io/en/latest/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## Quick start (Python)

```python
# Swap the import—everything else stays the same.
from sparkless.sql import SparkSession, functions as F

spark = SparkSession.builder.app_name("demo").get_or_create()
df = spark.createDataFrame([{"x": 1}, {"x": 2}])
df.filter(F.col("x") > 1).show()
```

Install from PyPI:

```bash
pip install "sparkless>=4,<5"
```

More Python docs (SQL/temp views, Delta, JDBC, testing plugin): see `python/README.md`.

---

## Why Sparkless (Python)?

- **Familiar API** — `SparkSession`, `DataFrame`, `Column`, and PySpark-like functions so you can reuse patterns without the JVM.
- **Fast local execution** — Runs natively (no JVM) and uses Polars for IO, expressions, and aggregations.
- **Test the same suite two ways** — Use `sparkless.testing` to run tests with Sparkless (fast) or real PySpark (parity checks).
- **Optional “Spark-like” features** — SQL, temp/global temp views, `saveAsTable`, Delta, and JDBC (see `python/README.md`).

---

## Features (Python surface)

| Area | What’s included |
|------|------------------|
| **Core** | `SparkSession`, `DataFrame`, `Column`, `functions` |
| **IO** | CSV, Parquet, JSON, Delta |
| **Expressions** | `col`, `lit`, `when`/`otherwise`, casts, null handling |
| **Aggregates** | `count`, `sum`, `avg`, `min`, `max`, `groupBy().agg()` |
| **Window** | `row_number`, `rank`, `dense_rank`, `lag`, `lead`, `first_value`, `last_value` via `.over()` |
| **Arrays, strings, JSON** | Common PySpark functions (`explode`, `regexp_*`, `get_json_object`, `from_json`, `to_json`, …) |
| **SQL + views** | `spark.sql`, temp/global temp views, `saveAsTable`, `catalog().listTables()` |
| **JDBC** | Read/write via `spark.read.jdbc(...)` / `df.write.jdbc(...)` |

**Parity:** 200+ fixtures validated against PySpark. Known differences: `docs/PYSPARK_DIFFERENCES.md`. Full parity status: `docs/PARITY_STATUS.md`. Out-of-scope items: `docs/DEFERRED_SCOPE.md`.

---

## Installation

### Python (`sparkless` v4)

Install from **PyPI**:

```bash
pip install "sparkless>=4,<5"
```

Or from this repo:

```bash
pip install ./python
```

See `python/README.md` for usage and development (including `maturin develop`).

### Rust engine (optional)

Most users should use the Python package above. If you want to embed the engine directly in Rust, depend on `robin-sparkless`.

Add to your `Cargo.toml`:

```toml
[dependencies]
robin-sparkless = "4"
```

Optional features:

```toml
robin-sparkless = { version = "4", features = ["sql"] }      # spark.sql(), temp views
robin-sparkless = { version = "4", features = ["delta"] }    # Delta Lake read/write
robin-sparkless = { version = "4", features = ["jdbc"] }     # PostgreSQL JDBC
robin-sparkless = { version = "4", features = ["sqlite"] }   # SQLite JDBC
robin-sparkless = { version = "4", features = ["jdbc_mysql"] } # MySQL/MariaDB JDBC
```

## Development

If you’re working on the Python package, start in `python/README.md` (it covers `maturin develop`, pytest, and dual-mode testing).

If you’re working on the Rust engine crates, see `docs/QUICKSTART.md` and the crate READMEs in `crates/`.

This repository contains:

- **`python/`**: the `sparkless` Python package (v4)
- **Rust crates**: the `robin-sparkless` engine (Cargo workspace)

| Command | Description |
|---------|-------------|
| `pip install ./python` | Install the Python package from this repo |
| `cd python && maturin develop` | Editable install for Python development |
| `pytest tests/ -v` | Run Python tests (sparkless backend) |
| `SPARKLESS_TEST_MODE=pyspark pytest tests/ -v` | Run the same tests against real PySpark |
| `scripts/typecheck_strict.sh` | Strict mypy for the Python `sparkless` package |
| `make check` | Rust engine: format, clippy, audit/deny, Rust tests |
| `make test-parity-phases` | Run PySpark parity fixtures (Rust engine) |

CI runs format, clippy, audit, deny, Rust tests, and parity tests on push/PR (see [.github/workflows/ci.yml](.github/workflows/ci.yml)).

---

## Documentation

| Resource | Description |
|----------|-------------|
| [**Python package**](python/README.md) | **Sparkless v4** — install from PyPI (`pip install sparkless`) or `pip install ./python`, quick start, Sparkless 3 vs 4.x, API overview |
| [**Read the Docs**](https://robin-sparkless.readthedocs.io/) | Full docs: quickstart, Rust usage, Python getting started, Sparkless integration (MkDocs) |
| [**docs.rs**](https://docs.rs/robin-sparkless) | Rust API reference |
| [QUICKSTART](docs/QUICKSTART.md) | Build, usage, optional features, benchmarks |
| [User Guide](docs/USER_GUIDE.md) | Everyday usage (Rust) |
| [Persistence Guide](docs/PERSISTENCE_GUIDE.md) | Global temp views, disk-backed saveAsTable |
| [UDF Guide](docs/UDF_GUIDE.md) | Scalar, vectorized, and grouped UDFs |
| [Testing Guide](docs/TESTING_GUIDE.md) | Dual-mode testing with `sparkless.testing` |
| [PySpark Differences](docs/PYSPARK_DIFFERENCES.md) | Known divergences |
| [Roadmap](docs/ROADMAP.md) | Development phases, Sparkless integration |
| [RELEASING](docs/RELEASING.md) | Publishing to crates.io and PyPI |

See [CHANGELOG.md](CHANGELOG.md) for version history.

---

## License

MIT
