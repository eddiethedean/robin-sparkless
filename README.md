# Sparkless — PySpark-compatible DataFrames without the JVM

Install: `pip install "sparkless>=4,<5"`

Use Sparkless to run PySpark-style unit tests and local pipelines **10–100× faster** in CI. Powered by the open-source Rust engine **robin-sparkless** (Polars execution).

> **Not a full cluster replacement.** See [Before you adopt](docs/BEFORE_YOU_ADOPT.md) for UDF, parity, and production caveats.

[![CI](https://github.com/eddiethedean/robin-sparkless/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/robin-sparkless/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/sparkless.svg)](https://pypi.org/project/sparkless/)
[![crates.io](https://img.shields.io/crates/v/robin-sparkless.svg)](https://crates.io/crates/robin-sparkless)
[![docs.rs](https://docs.rs/robin-sparkless/badge.svg)](https://docs.rs/robin-sparkless)
[![Documentation](https://readthedocs.org/projects/robin-sparkless/badge/?version=latest)](https://robin-sparkless.readthedocs.io/en/latest/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## Choose your path

| I want to… | Start here |
|------------|------------|
| **Use Sparkless in Python** (tests, local pipelines) | [python/README.md](python/README.md) · `pip install "sparkless>=4,<5"` |
| **Embed the Rust engine** | [docs/QUICKSTART.md](docs/QUICKSTART.md) · `robin-sparkless = "4"` on crates.io |
| **Contribute** | [CONTRIBUTING.md](CONTRIBUTING.md) · `make check-full` |

Full documentation: [Read the Docs](https://robin-sparkless.readthedocs.io/)

---

## Quick start (Python)

```python
# Swap the import—everything else stays the same.
from sparkless.sql import SparkSession, functions as F

spark = SparkSession.builder.app_name("demo").get_or_create()
df = spark.createDataFrame([{"x": 1}, {"x": 2}])
df.filter(F.col("x") > 1).show()
```

```bash
pip install "sparkless>=4,<5"
```

More: [Python getting started](docs/python_getting_started.md) · [Testing guide](docs/TESTING_GUIDE.md) · [FAQ](docs/FAQ.md)

---

## Why Sparkless (Python)?

- **Familiar API** — `SparkSession`, `DataFrame`, `Column`, and PySpark-like functions so you can reuse patterns without the JVM.
- **Fast local execution** — Runs natively (no JVM) and uses Polars for IO, expressions, and aggregations.
- **Test the same suite two ways** — Use `sparkless.testing` to run tests with Sparkless (fast) or real PySpark (parity checks).
- **Optional “Spark-like” features** — SQL, temp/global temp views, `saveAsTable`, Delta, and JDBC (see [python/README.md](python/README.md)).

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

**Parity:** 200+ fixtures validated against PySpark. [Before you adopt](docs/BEFORE_YOU_ADOPT.md) · [PySpark differences](docs/PYSPARK_DIFFERENCES.md) · [Parity status](docs/PARITY_STATUS.md)

---

## Installation

### Python (`sparkless` v4) — recommended

```bash
pip install "sparkless>=4,<5"
```

Contributors: `pip install ./python` or `cd python && maturin develop` — see [CONTRIBUTING.md](CONTRIBUTING.md).

### Rust engine (optional)

Most users should use the Python package above. To embed the engine in Rust:

```toml
[dependencies]
robin-sparkless = "4"
```

Optional features: `sql`, `delta`, `jdbc`, `sqlite`, `jdbc_mysql`. See [docs/QUICKSTART.md](docs/QUICKSTART.md).

---

## Development

**Prerequisites:** Rust (see [rust-toolchain.toml](rust-toolchain.toml)), Python 3.8+, maturin. Java only for `SPARKLESS_TEST_MODE=pyspark`.

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, `make check-full`, pytest, and maturin workflow.

| Command | Description |
|---------|-------------|
| `make check-full` | Full CI-equivalent check (Rust + Python) |
| `pytest tests/ -v` | Python tests (sparkless backend) |
| `SPARKLESS_TEST_MODE=pyspark pytest tests/ -v` | Same tests against real PySpark |
| `make check` | Rust: format, clippy, audit, tests |

---

## Documentation

| Resource | Description |
|----------|-------------|
| [**Python package**](python/README.md) | Install, quick start, platform matrix, API overview |
| [**Read the Docs**](https://robin-sparkless.readthedocs.io/) | Getting started, testing, migration, FAQ |
| [**Before you adopt**](docs/BEFORE_YOU_ADOPT.md) | UDF limits, parity caveats, production notes |
| [**CONTRIBUTING**](CONTRIBUTING.md) | Dev setup and PR checklist |
| [**docs.rs**](https://docs.rs/robin-sparkless) | Rust API reference |
| [Testing Guide](docs/TESTING_GUIDE.md) | Dual-mode testing with `sparkless.testing` |
| [PySpark Differences](docs/PYSPARK_DIFFERENCES.md) | Known divergences |
| [RELEASING](docs/RELEASING.md) | Publishing to crates.io and PyPI |

See [CHANGELOG.md](CHANGELOG.md) for version history.

---

## License

MIT
