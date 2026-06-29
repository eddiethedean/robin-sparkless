# Troubleshooting (Python)

Common issues when installing or running **Sparkless** (`pip install sparkless`).

## Installation

### `ImportError: No module named 'sparkless._native'`

The native extension failed to load. Try:

1. Reinstall: `pip install --force-reinstall "sparkless>=4,<5"`
2. Ensure Python version is **3.8+** and matches the wheel ABI (e.g. cp311 on Python 3.11).
3. On Linux **musl** (Alpine): use the musl wheel or build from source with `maturin develop`.

### No wheel for my platform

Build from source (requires Rust toolchain):

```bash
git clone https://github.com/eddiethedean/robin-sparkless.git
cd robin-sparkless/python
pip install maturin
maturin develop
```

See [Supported platforms](https://github.com/eddiethedean/robin-sparkless/blob/main/python/README.md#supported-platforms) in the package README.

### `maturin develop` fails

- Install Rust via [rustup](https://rustup.rs/) and use the repo-pinned toolchain (`rust-toolchain.toml`).
- On macOS: Xcode command-line tools may be required.
- Run from `python/` directory, not repo root.

## Runtime

### Behavior differs from PySpark

Expected for some APIs. Check [PySpark differences](PYSPARK_DIFFERENCES.md) and [Parity status](PARITY_STATUS.md). Run the same test with `SPARKLESS_TEST_MODE=pyspark` to compare.

### Python UDF fails or is unsupported

Python `@udf` / pandas UDFs are **not supported**. Use built-in `functions` or engine Rust UDFs ([UDF guide](UDF_GUIDE.md)).

### `spark.sql()` errors on DDL/DML

Only a subset of Spark SQL is implemented. See [PySpark differences — SQL](PYSPARK_DIFFERENCES.md).

### JDBC connection failures

- Verify driver URL and credentials.
- For hardened deployments: `SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL=false` requires `dbtable` (no arbitrary SQL). See [Production deployment](PRODUCTION.md).

## Testing

### `SPARKLESS_TEST_MODE=pyspark` fails

Requires:

```bash
pip install "sparkless[pyspark]"
# Java installed (JAVA_HOME set)
SPARKLESS_TEST_MODE=pyspark pytest tests/ -v
```

### Tests pass locally but fail in CI

- Pin `sparkless` version: `pip install "sparkless>=4.13,<4.14"`
- Ensure CI uses a supported platform (see [platform matrix](https://github.com/eddiethedean/robin-sparkless/blob/main/python/README.md#supported-platforms)).
- For multiprocessing: call `sparkless.configure_for_multiprocessing()` in worker processes if needed.

### Slow first test run

Native extension and session startup are one-time costs. Use `pytest-xdist` (`pytest -n auto`) for parallel runs.

## Getting help

- [FAQ](FAQ.md)
- [GitHub Issues](https://github.com/eddiethedean/robin-sparkless/issues) — include Sparkless version, Python version, OS, and minimal repro.
