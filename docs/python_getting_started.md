# Getting Started (Python — Sparkless v4)

Use this guide for the in-process Python API. If your tests import
`pyspark.sql` and you want to keep that client, follow the
[Spark Connect quickstart](SPARK_CONNECT.md).

This guide is for the **Sparkless v4** Python package: a PySpark-like API backed by the [robin-sparkless](https://github.com/eddiethedean/robin-sparkless) Rust engine. Sparkless 3.x uses the Polars **Python** package; 4.x uses the **Rust** crate (no Polars Python at runtime).

## First run

Save this example as `first_dataframe.py`.

```python
from sparkless.sql import SparkSession, functions as F

spark = SparkSession.builder.app_name("first-run").get_or_create()
try:
    people = spark.createDataFrame(
        [("Alice", 25), ("Bob", 30)], ["name", "age"]
    )
    result = people.filter(F.col("age") >= 30).select("name")
    assert [row["name"] for row in result.collect()] == ["Bob"]
    result.show()
finally:
    spark.stop()
```

From the directory containing the file, install Sparkless in a Python 3.11
virtual environment and run it with the same interpreter. On macOS or Linux:

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install "sparkless==4.19.0"
.venv/bin/python first_dataframe.py
```

On Windows PowerShell:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install "sparkless==4.19.0"
.\.venv\Scripts\python.exe first_dataframe.py
```

Expect one row with the name `Bob`.
No Java, PySpark, or Polars Python install is required for this local example.
For wheel support by OS and architecture, see the
[installation guide](INSTALLATION.md#platforms-and-build-features).

## Installation

> **Before you adopt:** Read [Before you adopt](BEFORE_YOU_ADOPT.md) for UDF limits, parity gaps, and production caveats.

### From PyPI (recommended)

```bash
python -m pip install "sparkless==4.19.0"
```

Requires **Python 3.11+**. Local execution uses a native extension and needs
neither Java, PySpark, nor the Polars Python package.

### Supported platforms

Local engine wheels cover CPython 3.11–3.14 on Linux, macOS, and Windows.
The optional server targets Linux x86_64 with glibc 2.28+ and macOS 14+ on
Apple Silicon. See the [platform and build matrix](INSTALLATION.md#platforms-and-build-features)
for architectures, libc, and feature details.

### Optional extras

The table describes 4.19.0. See [installation and versions](INSTALLATION.md) for package names and platform requirements.

| Extra | Dependencies and purpose |
| --- | --- |
| `sparkless[dev]` | Development tools: pandas, PyArrow, Polars, pytest-xdist, and related test tools |
| `sparkless[testing]` | Backend-independent pytest fixtures and helpers from `sparkless-testing` |
| `sparkless[pyspark]` | PySpark 3.5 and its Delta integration; requires a compatible Java runtime |
| `sparkless[pyspark4]` | PySpark 4 and its Delta integration; requires a compatible Java runtime |
| `sparkless[server]` | The exactly matching base `sparkless-server` product |
| `sparkless[server-duckdb]` | The exactly matching DuckDB-source `sparkless-server-duckdb` product |
| `sparkless[connect]` | Base server and pinned `pyspark-client==4.0.5` |
| `sparkless[connect-duckdb]` | DuckDB server and pinned `pyspark-client==4.0.5` |
| `sparkless-connect[client]` | Shared remote client/launcher, without either server binary |
| `sparkless[jdbc]` | Database integration test tools, testcontainers, and database drivers |

For example:

```bash
python -m pip install "sparkless[testing]==4.19.0"
```

Use separate environments for `pyspark` and `pyspark4`; they require different
Spark versions. The `jdbc` extra supplies test dependencies rather than enabling
every native database backend. Backend support and options depend on the build
and connection configuration.

For an existing server endpoint, install `sparkless-connect[client]` and use
`pyspark.sql.SparkSession.builder.remote(...)`. To own a local base server,
install `sparkless-server` and import `SparklessServer` from `sparkless_server`.
To use trusted read-only DuckDB aliases, install `sparkless-server-duckdb` and
import its product-bound helper from `sparkless_server_duckdb`. Both 4.19 server
products run transformations through Polars; the DuckDB product adds table
sources and its matching Quack extension.

### From source (contributors)

Use the [contributor setup](https://github.com/eddiethedean/robin-sparkless/blob/main/CONTRIBUTING.md#setup)
to create the repository-root uv environment, install development dependencies,
and build the native extension with Maturin. Rebuild after changing Rust or PyO3
code; an editable Python install alone does not rebuild the native library.

## Importing the API

### PySpark-style import swap

For supported DataFrame code, change your imports to use Sparkless. Validate
behavior against PySpark; see [Before you adopt](BEFORE_YOU_ADOPT.md).

```python
# Before (PySpark)
from pyspark.sql import SparkSession

# After (Sparkless v4)
from sparkless.sql import SparkSession
```

Create a session with `SparkSession.builder.app_name("...").get_or_create()`.
Check the [known differences](PYSPARK_DIFFERENCES.md) before assuming API or
behavioral equivalence.

## Core Features

The examples below reuse `spark` and `df` created in **DataFrame Operations**.
Run them in that same Python session, or copy the setup block first.

### DataFrame Operations

```python
from sparkless.sql import SparkSession, functions as F

spark = SparkSession.builder.app_name("Example").get_or_create()
data = [
    {"name": "Alice", "dept": "Engineering", "salary": 80000},
    {"name": "Bob", "dept": "Sales", "salary": 75000},
    {"name": "Charlie", "dept": "Engineering", "salary": 90000},
]
df = spark.createDataFrame(data)

# Filter and select
high_earners = df.filter(F.col("salary") > 75000)
names = df.select("name", "dept")

# Aggregations
dept_avg = df.groupBy("dept").avg("salary")
```

### Window Functions

```python
from sparkless.sql import functions as F
from sparkless.sql.window import Window

window_spec = Window.partitionBy("dept").orderBy(F.desc("salary"))
ranked = df.withColumn("rank", F.row_number().over(window_spec))
```

### SQL Queries

```python
df.createOrReplaceTempView("employees")
result = spark.sql("SELECT name, salary FROM employees WHERE salary > 80000")
result.show()
```

### Lazy Evaluation

Transformations (filter, select, join, etc.) are queued; execution happens on **actions** (`collect()`, `count()`, `show()`, `write`), matching PySpark’s model.

## Testing with Sparkless v4

Follow the [pytest quickstart](TESTING_GUIDE.md#quick-start) to install the
`[testing]` extra, register the plugin, and run your first test. Use the same
virtual environment as the first run. The guide also shows how to run the same
test against PySpark in a separate environment.

## Read and write files

The in-process API supports CSV, JSON, Parquet, and Delta Lake. This example
writes Parquet and reads it back without leaving files behind:

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
        assert spark.read.parquet(path).count() == 2
finally:
    spark.stop()
```

For writer options, saved-table persistence, and database-specific behavior,
see [PySpark differences](PYSPARK_DIFFERENCES.md). Arrow table and bounded batch
exports are covered in the [Python API reference](PYTHON_API_REFERENCE.md#actions-and-arrow).

## Native server and Spark Connect

In 4.19, `SparklessConnect` can own both a server process and a PySpark
Connect session. Follow the [Spark Connect quickstart](SPARK_CONNECT.md)
for a base server, or [read DuckDB tables](DUCKDB_SOURCE.md) for the optional
source-enabled product. The remote API has a narrower support contract than
the in-process Python API on this page.

## Performance

Workload size, query plans, startup costs, and the host machine affect runtime.
Measure representative workloads in your own environment; this guide does not
promise a fixed speedup over Spark. `collect()` materializes the complete result
in Python, and bounded Arrow batches do not bound total query memory.

## Next Steps

- [Testing Guide](TESTING_GUIDE.md) — Full guide to `sparkless_testing` (dual-mode testing, fixtures, comparison utilities)
- [Python API reference](PYTHON_API_REFERENCE.md) — API overview and Arrow export
- [Spark Connect](SPARK_CONNECT.md) — Native server and remote API contract
- [PySpark differences](PYSPARK_DIFFERENCES.md) — Known divergences and caveats
- [Migration (PySpark / Sparkless 3)](python_migration.md) — Switching from PySpark or Sparkless 3.x
- [Parity status](PARITY_STATUS.md) — Coverage and fixture status

## Getting Help

- **Repository:** [github.com/eddiethedean/robin-sparkless](https://github.com/eddiethedean/robin-sparkless)
- **Issues:** [github.com/eddiethedean/robin-sparkless/issues](https://github.com/eddiethedean/robin-sparkless/issues)
- **Sparkless 3.x (Polars Python):** [github.com/eddiethedean/sparkless](https://github.com/eddiethedean/sparkless)
