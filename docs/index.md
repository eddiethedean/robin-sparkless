# Sparkless / Robin Sparkless

**PySpark-compatible DataFrames without the JVM.** The **`sparkless`** Python package is the primary adoption path for testing and local development. The **`robin-sparkless`** Rust crate powers it (and can be embedded directly).

Install Python package: `pip install "sparkless>=4,<5"`

> **Evaluating Sparkless?** Read [Before you adopt](BEFORE_YOU_ADOPT.md) first — UDF limits, parity gaps, and production caveats.

---

## Choose your path

| Persona | Start here |
|---------|------------|
| **Python user / tester** | [Getting started (Python)](python_getting_started.md) · [Package README](https://github.com/eddiethedean/robin-sparkless/blob/main/python/README.md) |
| **Rust embedder** | [Quickstart](QUICKSTART.md) · [User guide](USER_GUIDE.md) · [docs.rs](https://docs.rs/robin-sparkless) |
| **Contributor** | [CONTRIBUTING.md](https://github.com/eddiethedean/robin-sparkless/blob/main/CONTRIBUTING.md) |
| **Enterprise / production** | [Before you adopt](BEFORE_YOU_ADOPT.md) · [Production deployment](PRODUCTION.md) |

---

## Python (Sparkless v4)

- [Getting started](python_getting_started.md) — Install from PyPI, quick start, core features
- [Testing guide](TESTING_GUIDE.md) — Dual-mode pytest with `sparkless.testing`
- [Migration](python_migration.md) — From PySpark or Sparkless 3.x
- [FAQ](FAQ.md) · [Troubleshooting (Python)](TROUBLESHOOTING_PYTHON.md)
- [Python API reference (starter)](PYTHON_API_REFERENCE.md)
- [PySpark differences](PYSPARK_DIFFERENCES.md) · [Parity status](PARITY_STATUS.md)

## Rust (robin-sparkless)

Robin Sparkless provides a **PySpark-like API** in Rust using [Polars](https://www.pola.rs/) for execution. Sparkless v4 calls it via PyO3.

| Feature | Description |
|--------|-------------|
| **Core** | `SparkSession`, `DataFrame`; lazy by default. ExprIr (engine-agnostic) and Column API (Polars-backed) |
| **Engine** | Polars for fast, native execution |
| **Optional** | SQL, Delta Lake, JDBC |

- [Quickstart](QUICKSTART.md) · [User guide](USER_GUIDE.md) · [Persistence guide](PERSISTENCE_GUIDE.md)
- [Embedding](EMBEDDING.md) · [UDF guide](UDF_GUIDE.md)
- [Rust API on docs.rs](https://docs.rs/robin-sparkless)

## Product history

Sparkless **3.x** (Polars Python backend) and **4.x** (this repo, Rust backend) — see [Product history](PRODUCT_HISTORY.md).

## License

MIT
