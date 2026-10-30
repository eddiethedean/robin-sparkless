# Sparkless

**PySpark-style DataFrames in Python. Native Rust execution. No JVM.**

[![PyPI](https://img.shields.io/pypi/v/sparkless.svg)](https://pypi.org/project/sparkless/)
[![Python versions](https://img.shields.io/pypi/pyversions/sparkless.svg)](https://pypi.org/project/sparkless/)
[![Documentation](https://readthedocs.org/projects/robin-sparkless/badge/?version=latest)](https://robin-sparkless.readthedocs.io/en/latest/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/eddiethedean/robin-sparkless/blob/main/LICENSE)

Sparkless brings familiar `SparkSession`, `DataFrame`, `Column`, and SQL APIs to
local Python development and testing. Build transformations, work with local
files, and test pipeline logic without installing Java or starting a Spark cluster.

Version 4 runs on **robin-sparkless**, a Rust engine built on Polars. The Python
package calls a native extension; local execution requires neither PySpark nor
the Polars Python package. For applications that need a separate process,
Sparkless also provides an optional native HTTP and Spark Connect server.

[Install](#installation) · [Quick start](#quick-start) ·
[Test your pipelines](#test-your-pipelines) · [Spark Connect](#spark-connect-in-4180) ·
[Documentation](#documentation)

## Why use Sparkless?

- **Keep familiar DataFrame patterns.** Use filters, joins, aggregations,
  windows, built-in functions, and SQL through a PySpark-style API.
- **Simplify local setup and CI.** Install a Python wheel and execute locally
  through Rust, without a Java runtime or Spark installation.
- **Check your code against Spark.** Use `sparkless.testing` to run the same
  pytest tests with Sparkless and real PySpark.
- **Choose an execution boundary.** Run in process or own a native server child
  process with explicit startup, readiness, and cleanup.

Sparkless runs on one machine and implements a subset of PySpark behavior.
Validate the APIs and semantics your application relies on against real Spark.
The [adoption guide](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/BEFORE_YOU_ADOPT.md)
and [known differences](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PYSPARK_DIFFERENCES.md)
explain where it fits.

## Installation

Requires **Python 3.11+**:

```bash
python -m pip install "sparkless>=4,<5"
```

### Supported platforms

The release workflow builds `cp311-abi3` extension wheels for Python 3.11–3.14:

| Platform | Architectures |
| --- | --- |
| Linux, glibc or musl | x86_64, ARM64 |
| macOS | Intel, Apple Silicon |
| Windows | x86_64, ARM64 |

Check the [PyPI file inventory](https://pypi.org/project/sparkless/#files) for the
release you install. If no wheel matches your environment, follow the
[source build instructions](https://github.com/eddiethedean/robin-sparkless/blob/main/CONTRIBUTING.md#setup).
The optional server has a smaller platform set, described below.

### Optional extras

| Extra | Installs dependencies for… |
| --- | --- |
| `sparkless[dev]` | Development and tests, including pytest, pandas, PyArrow, Polars, and pytest-xdist |
| `sparkless[pyspark]` | Tests against PySpark 3.5 and the matching Delta integration; requires Java |
| `sparkless[pyspark4]` | Tests against PySpark 4 and the matching Delta integration; requires Java |
| `sparkless[server]` | The matching platform-specific `sparkless-server-bin` package |
| `sparkless[jdbc]` | Database integration tests, including testcontainers and database drivers |

Install extras with the same version constraint, for example
`python -m pip install "sparkless[dev]>=4,<5"`. Use separate environments for the
`pyspark` and `pyspark4` extras, which require different Spark versions. The
`jdbc` extra supplies test dependencies; database support and options depend on
the backend and connection configuration.

## Quick start

Create a dataset and summarize orders by region:

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

    # Query the same data through a temporary view.
    orders.createOrReplaceTempView("orders")
    assert spark.sql("SELECT SUM(amount) AS total FROM orders").collect()[0]["total"] == 230
finally:
    spark.stop()
```

Transformations extend a lazy query plan. Actions such as `collect()`, `count()`,
`show()`, and writes execute it through the native engine. `collect()` materializes
the result in Python, so size local workloads for your machine's memory and CPU.

### API at a glance

These are **in-process Python** capabilities. Spark Connect has a separate,
narrower contract described below.

| Area | Examples |
| --- | --- |
| Sessions | `createDataFrame`, `range`, `sql`, temporary views, and saved tables |
| DataFrames | Select, filter, joins, unions, sorting, grouping, pivot, and aggregations |
| Expressions | Built-in functions for strings, arrays, JSON, dates, casts, and null handling |
| Windows | Ranking, offsets, and aggregate expressions over windows |
| Storage | CSV, JSON, Parquet, Delta Lake, and JDBC-style database access |
| Arrow | Materialized tables and row/byte-bounded RecordBatch iteration |

### Read and write files

This example writes Parquet and reads it back in a temporary directory:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from sparkless.sql import SparkSession

spark = SparkSession.builder.app_name("files").get_or_create()
try:
    with TemporaryDirectory() as directory:
        path = str(Path(directory) / "orders.parquet")
        orders = spark.createDataFrame([(1, 120), (2, 80)], ["id", "amount"])
        orders.write.mode("overwrite").parquet(path)

        restored = spark.read.parquet(path)
        assert restored.count() == 2
finally:
    spark.stop()
```

CSV, JSON, Delta Lake, and JDBC-style database access are also available. See
[getting started](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/python_getting_started.md)
and [PySpark differences](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PYSPARK_DIFFERENCES.md)
for supported options, persistence behavior, and backend requirements.

### Export bounded Arrow batches

Install PyArrow with `python -m pip install pyarrow`. Use `toArrow()` for a
materialized PyArrow Table, or `toArrowBatches()` to consume RecordBatches with
explicit row and byte limits:

```python
from sparkless.sql import SparkSession

spark = SparkSession.builder.app_name("arrow").get_or_create()
try:
    df = spark.range(10_000)
    with df.toArrowBatches(max_rows=1024, max_bytes=8 * 1024 * 1024) as batches:
        rows = sum(batch.num_rows for batch in batches)
    assert rows == 10_000
finally:
    spark.stop()
```

Each batch respects both limits; the byte limit is checked against
`RecordBatch.nbytes`. A row larger than `max_bytes` raises `ValueError`. Defaults
are 10,000 rows and 64 MiB per batch. Use the context manager, especially when
stopping early, to close the iterator and stop further batch production. Batch
limits do not bound the entire query's intermediate memory.

## Test your pipelines

The `sparkless.testing` pytest plugin provides sessions and imports for the
selected backend. Install pytest alongside Sparkless:

```bash
python -m pip install "sparkless>=4,<5" pytest
```

Register the plugin in your project's `conftest.py`:

```python
pytest_plugins = ["sparkless.testing"]
```

Create `test_orders.py` using the `spark` and `spark_imports` fixtures:

```python
def test_positive_orders(spark, spark_imports):
    F = spark_imports.F
    orders = spark.createDataFrame([(120,), (-10,), (30,)], ["amount"])
    positive = orders.filter(F.col("amount") > 0)

    assert positive.count() == 2
    assert positive.agg(F.sum("amount")).collect()[0][0] == 150
```

Run the test with Sparkless, then check it against real PySpark:

```bash
python -m pytest test_orders.py

# The PySpark run also requires Java compatible with your installed Spark version.
python -m pip install "sparkless[pyspark]>=4,<5"
SPARKLESS_TEST_MODE=pyspark python -m pytest test_orders.py
```

The plugin also supplies isolated sessions, backend markers, and DataFrame
comparison helpers. See the
[testing guide](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/TESTING_GUIDE.md)
for details. Compare values and schemas, and specify ordering when it matters.

## Spark Connect in 4.18.0

The optional native server exposes HTTP and a supported subset of the **Spark
4.0.5 Connect** protocol. Its Rust process needs neither a Python worker nor a
JVM. Connect from Python using the standalone `pyspark-client` package.

The example below targets the 4.18.0 distribution. Before publication, install
the matching candidate wheels in place of the PyPI server installation.

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

`SparklessServer` owns the child process, waits for HTTP and gRPC readiness,
allocates loopback ports, and stops and reaps the child on context exit.
`server.http_url` exposes the HTTP endpoint. `AsyncSparklessServer` supports the
same lifecycle with `async with`.

Pass `connect_port=None` for HTTP-only operation; accessing `spark_connect_url`
then raises `UnsupportedSparkConnectError`. An explicit `executable_path` can
select a matching locally built binary. The launcher uses the installed server
wheel by default and does not download or compile an executable at runtime.

Prebuilt **server** wheels support Linux x86_64 with glibc 2.28+ and macOS 14+
on Apple Silicon. These differ from the Python extension wheel platforms.

**Connect scope:** ranges, bounded scalar local relations with explicit schemas,
supported projections and filters, limits, global count, and the documented
analysis and lifecycle RPCs. The broader in-process Python API described above
does not imply remote support for joins, sorting, windows, grouped aggregation,
SQL commands, file I/O, or UDFs. Unsupported plans return errors.

Read the [Spark Connect guide](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/SPARK_CONNECT.md)
for supported types, request coverage, result/replay budgets, and deployment
boundaries. Arrow response streams do not provide Structured Streaming.

## Compatibility and limits

- **PySpark semantics:** the local API defaults to a PySpark 3.5-like profile,
  with an opt-in 4.0 profile. These profiles do not establish complete version
  parity. See [compatibility profiles](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PYSPARK_COMPAT_PROFILES.md).
- **Python transformations:** use built-in expressions. Python `@udf` and pandas
  UDF execution are unsupported. Rust extension points are documented in the
  [UDF guide](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/UDF_GUIDE.md).
- **Single-machine execution:** there is no distributed scheduler or executor
  pool. RDD execution, MLlib, and Structured Streaming are outside the supported
  local execution surface.
- **API differences:** some methods accept options without implementing Spark's
  behavior. Review the [known differences](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PYSPARK_DIFFERENCES.md)
  and [parity status](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PARITY_STATUS.md).

For application deployment, review the
[production guide](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PRODUCTION.md)
and configure file access, database permissions, resource limits, and network
exposure for your environment.

## Documentation

| Resource | Start here for… |
| --- | --- |
| [Documentation site](https://robin-sparkless.readthedocs.io/) | Guides and reference material |
| [Getting started](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/python_getting_started.md) | Sessions, DataFrames, SQL, and I/O |
| [Before you adopt](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/BEFORE_YOU_ADOPT.md) | Fit, caveats, and adoption decisions |
| [Migration guide](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/python_migration.md) | Moving from PySpark or Sparkless 3.x |
| [Testing guide](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/TESTING_GUIDE.md) | Running one test suite on both backends |
| [Spark Connect](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/SPARK_CONNECT.md) | Server setup and the 4.18 remote contract |
| [FAQ](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/FAQ.md) | Common questions |
| [Changelog](https://github.com/eddiethedean/robin-sparkless/blob/main/CHANGELOG.md) | Release history |

## Contribute

The [robin-sparkless repository](https://github.com/eddiethedean/robin-sparkless)
contains the Python package, native engine, and test suites. Follow the
[contributor setup](https://github.com/eddiethedean/robin-sparkless/blob/main/CONTRIBUTING.md#setup)
for the uv environment, pinned Rust toolchain, and native extension build.
Report bugs or request features in
[GitHub Issues](https://github.com/eddiethedean/robin-sparkless/issues).

Sparkless is available under the
[MIT license](https://github.com/eddiethedean/robin-sparkless/blob/main/LICENSE).
