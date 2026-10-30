# Sparkless

**PySpark-style DataFrames. Native Rust execution. No JVM.**

[![CI](https://github.com/eddiethedean/robin-sparkless/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/robin-sparkless/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/sparkless.svg)](https://pypi.org/project/sparkless/)
[![crates.io](https://img.shields.io/crates/v/robin-sparkless.svg)](https://crates.io/crates/robin-sparkless)
[![docs.rs](https://docs.rs/robin-sparkless/badge.svg)](https://docs.rs/robin-sparkless)
[![Documentation](https://readthedocs.org/projects/robin-sparkless/badge/?version=latest)](https://robin-sparkless.readthedocs.io/en/latest/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Sparkless brings familiar `SparkSession`, `DataFrame`, `Column`, and SQL APIs to
local Python development and testing. Run transformations, read files, and test
pipeline logic without installing Java or starting a Spark cluster.

The Python package is **`sparkless`**. Its execution engine is **`robin-sparkless`**,
a Rust library built on Polars. You can use the engine directly from Python,
embed it in Rust, or run the native server through HTTP and a supported subset
of Spark Connect.

[Get started](#get-started-in-python) · [Test your pipelines](#test-your-pipelines) ·
[Spark Connect](#spark-connect-in-4180) · [Rust](#embed-the-rust-engine) ·
[Documentation](#documentation)

## Why use Sparkless?

- **Keep familiar DataFrame code.** Use filters, joins, aggregations, windows,
  built-in functions, and SQL with a PySpark-style interface.
- **Simplify local development and CI.** The Python package executes through a
  native extension; its local mode needs neither Java nor a Spark installation.
- **Check behavior against Spark.** The `sparkless.testing` pytest plugin lets
  you run the same tests with Sparkless and real PySpark.
- **Choose how to integrate.** Work in process, own a native server child process,
  or embed the Rust engine in your application.

Sparkless runs on a single machine. Compatibility depends on the APIs and
semantics your application uses; validate representative workloads against
PySpark before relying on equivalent results. Start with
[Before you adopt](docs/BEFORE_YOU_ADOPT.md) and the
[known differences](docs/PYSPARK_DIFFERENCES.md).

## Get started in Python

Requires **Python 3.11+**. Install the Python package:

```bash
python -m pip install "sparkless>=4,<5"
```

Create a session and summarize a small dataset:

```python
from sparkless.sql import SparkSession, functions as F

spark = SparkSession.builder.app_name("orders").get_or_create()
try:
    orders = spark.createDataFrame(
        [("north", 120), ("south", 80), ("north", 30)],
        ["region", "amount"],
    )

    totals = (
        orders.filter(F.col("amount") > 0)
        .groupBy("region")
        .agg(F.sum("amount").alias("total"))
        .orderBy("region")
    )
    totals.show()

    assert [(row["region"], row["total"]) for row in totals.collect()] == [
        ("north", 150),
        ("south", 80),
    ]
finally:
    spark.stop()
```

Transformations extend a lazy query plan. Actions such as `collect()`, `count()`,
`show()`, and writes execute it through the native engine. Local execution uses
your machine's memory and CPU; `collect()` materializes the result in Python.

Continue with the [Python getting started guide](docs/python_getting_started.md),
[Python package reference](python/README.md), or
[migration guide](docs/python_migration.md).

### What can I use?

These capabilities describe the **in-process Python API**. The remote Connect
endpoint has a separate, narrower contract.

| Area | Examples |
| --- | --- |
| DataFrames | Select, filter, joins, unions, sorting, grouping, pivot, and aggregations |
| Expressions | Built-in functions for strings, arrays, JSON, dates, casts, and null handling |
| Windows | Ranking, offsets, and aggregate expressions over windows |
| SQL and catalogs | `spark.sql()`, temporary/global temporary views, and saved tables |
| Files and storage | CSV, JSON, Parquet, and Delta Lake reads and writes |
| Database access | JDBC-style reads and writes; backend options and requirements vary |
| Arrow export | Materialized `toArrow()` and row/byte-bounded `toArrowBatches()`; requires PyArrow |

See [parity status](docs/PARITY_STATUS.md) and
[PySpark differences](docs/PYSPARK_DIFFERENCES.md) for coverage, limitations, and
methods that accept options without implementing Spark's behavior.

### Python wheel platforms

The release workflow builds `cp311-abi3` extension wheels for Python 3.11–3.14:

| Platform | Architectures |
| --- | --- |
| Linux, glibc or musl | x86_64, ARM64 |
| macOS | Intel, Apple Silicon |
| Windows | x86_64, ARM64 |

Check the [PyPI file inventory](https://pypi.org/project/sparkless/#files) for the
release you install. If your platform has no matching wheel, use the
[source build instructions](CONTRIBUTING.md#setup).

## Test your pipelines

Use the `sparkless.testing` plugin to share pytest tests between Sparkless and
PySpark. Install the test dependencies:

```bash
python -m pip install "sparkless>=4,<5" pytest
```

Register the plugin in your project's `conftest.py`:

```python
pytest_plugins = ["sparkless.testing"]
```

Write a test in `test_orders.py`. The fixtures provide a session and imports
for the selected backend:

```python
def test_positive_orders(spark, spark_imports):
    F = spark_imports.F
    orders = spark.createDataFrame([(120,), (-10,), (30,)], ["amount"])
    positive = orders.filter(F.col("amount") > 0)

    assert positive.count() == 2
    assert positive.agg(F.sum("amount")).collect()[0][0] == 150
```

Run locally with Sparkless, then check the same test against real PySpark:

```bash
python -m pytest test_orders.py

# The PySpark run also requires Java compatible with your installed Spark version.
python -m pip install "sparkless[pyspark]>=4,<5"
SPARKLESS_TEST_MODE=pyspark python -m pytest test_orders.py
```

See the [testing guide](docs/TESTING_GUIDE.md) for fixtures, comparison helpers,
backend markers, and compatibility profiles. Tests should compare values and
schemas, and specify ordering when it matters.

## Spark Connect in 4.18.0

The native server exposes HTTP and an optional **Spark 4.0.5 Connect** endpoint.
Use the standalone `pyspark-client` package to connect through PySpark's remote
API while execution stays in Rust. The server process needs neither a Python
worker nor a JVM.

The following example targets the 4.18.0 distribution. Before publication,
install the matching candidate wheels in place of the PyPI server installation.

```bash
python -m pip install "sparkless[server]==4.18.0" "pyspark-client==4.0.5"
```

```python
from pyspark.sql import SparkSession
from sparkless import SparklessServer

with SparklessServer() as server:
    spark = SparkSession.builder.remote(server.spark_connect_url).create()
    try:
        assert spark.range(10).filter("id > 5").count() == 4
    finally:
        spark.stop()
```

`SparklessServer` starts the child process, waits for HTTP and gRPC readiness,
allocates loopback ports, and stops and reaps the child when the context exits.
`AsyncSparklessServer` provides the equivalent `async with` lifecycle. Pass
`connect_port=None` for HTTP-only operation.

The prebuilt **server** wheels support Linux x86_64 with glibc 2.28+ and macOS
14+ on Apple Silicon. This is a smaller platform set than the Python extension.

**Connect scope:** 4.18 supports ranges, bounded scalar local relations with
explicit schemas, supported projections and filters, limits, global count,
and the documented analysis and lifecycle RPCs. Joins, sorting, windows,
general grouped aggregation, SQL commands, file I/O, and UDFs are outside the
4.18 Connect contract. Unsupported plans return errors. Arrow response streams
do not provide Structured Streaming.

Read the [Spark Connect guide](docs/SPARK_CONNECT.md) for the complete supported
types, request surface, result/replay limits, and deployment boundaries.

## Compatibility and boundaries

- **Validate Spark semantics.** The local API defaults to a PySpark 3.5-like
  compatibility profile, with an opt-in 4.0 profile. Profiles do not establish
  complete version parity. See [compatibility profiles](docs/PYSPARK_COMPAT_PROFILES.md).
- **Use built-in expressions for Python transformations.** Python `@udf` and
  pandas UDF execution are unsupported. Rust extension points are described in
  the [UDF guide](docs/UDF_GUIDE.md).
- **Keep distributed workloads on Spark.** Sparkless has no distributed
  scheduler or executors. RDD execution, MLlib, and Structured Streaming are
  outside its supported local execution surface.
- **Review deployment assumptions.** Local files, database access, memory
  budgets, and network exposure need application-specific configuration. See
  [production deployment](docs/PRODUCTION.md) and the server guide before
  exposing a service beyond loopback.

## Embed the Rust engine

The `robin-sparkless` crate exposes the native DataFrame engine without requiring
the Python package. Add it to your application:

```toml
[dependencies]
robin-sparkless = "4"
```

Optional Cargo features include `sql`, `delta`, `jdbc`, `sqlite`, and database
backend features. The [Rust quickstart](docs/QUICKSTART.md) covers the
engine-independent expression API and Polars-backed integration;
[docs.rs](https://docs.rs/robin-sparkless) provides the API reference.

**4.18.0 Rust security notice:** ordinary crates.io consumers retain a temporary
zstd advisory exception through **2026-11-26**. Apply the pinned consumer Cargo
patches when handling untrusted compressed input. Official Python wheels,
prebuilt server wheels, containers, and the Python source archive use the fixed
dependency graph. See the
[exposure, expiry, and patch instructions](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/security/zstd-4.18-exception.md).

## Contribute

Start with [CONTRIBUTING.md](CONTRIBUTING.md) for the uv environment, pinned Rust
toolchain, native extension build, and focused checks. After setup, run checks
from the repository root:

```bash
# Rust formatting, Clippy, dependency checks, Rust tests, and Python lint/types
make check-full

# Python tests and parity fixtures
uv run --no-project --python .venv/bin/python python -m pytest tests/ -v
make test-parity-phases
```

The repository contains the Python bindings under `python/`, the Rust workspace
under `crates/` and `src/`, and the test suites under `tests/`. Maintainers can
find package validation and publication steps in the [release guide](docs/RELEASING.md).

## Documentation

| Resource | Start here for… |
| --- | --- |
| [Documentation site](https://robin-sparkless.readthedocs.io/) | Guides and reference material |
| [Python package](python/README.md) | Python installation, API examples, and optional extras |
| [Before you adopt](docs/BEFORE_YOU_ADOPT.md) | Fit, caveats, and adoption decisions |
| [Migration guide](docs/python_migration.md) | Moving from PySpark or Sparkless 3.x |
| [Testing guide](docs/TESTING_GUIDE.md) | Running one test suite on both backends |
| [Spark Connect](docs/SPARK_CONNECT.md) | Native server setup and the 4.18 remote contract |
| [Rust quickstart](docs/QUICKSTART.md) | Embedding the engine |
| [FAQ](docs/FAQ.md) | Common questions |
| [Changelog](CHANGELOG.md) | Release history |

Report bugs and request features in [GitHub Issues](https://github.com/eddiethedean/robin-sparkless/issues).
Sparkless is available under the [MIT license](LICENSE).
