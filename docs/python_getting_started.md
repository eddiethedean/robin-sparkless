# Getting Started (Python — Sparkless v4)

See [versions/install paths](INSTALLATION.md) and the [4.18 surface contract](SUPPORT_MATRIX.md) before selecting features.

This guide is for the **Sparkless v4** Python package: a PySpark-like API backed by the [robin-sparkless](https://github.com/eddiethedean/robin-sparkless) Rust engine. Sparkless 3.x uses the Polars **Python** package; 4.x uses the **Rust** crate (no Polars Python at runtime).

## Installation

> **Before you adopt:** Read [Before you adopt](BEFORE_YOU_ADOPT.md) for UDF limits, parity gaps, and production caveats.

### From PyPI (recommended)

```bash
python -m pip install "sparkless>=4,<5"
```

Requires **Python 3.11+**. Local execution uses a native extension and needs
neither Java, PySpark, nor the Polars Python package.

### Supported platforms

The release workflow builds `cp311-abi3` extension wheels for Python 3.11–3.14:

| Platform | Architectures |
| --- | --- |
| Linux, glibc or musl | x86_64, ARM64 |
| macOS | Intel, Apple Silicon |
| Windows | x86_64, ARM64 |

Check the [PyPI file inventory](https://pypi.org/project/sparkless/#files) for
your release. A platform without a matching wheel requires a source build.
The optional native server has a smaller platform set: Linux x86_64 with glibc
2.28+ and macOS 14+ on Apple Silicon. See [Spark Connect](SPARK_CONNECT.md).

### Optional extras

| Extra | Dependencies and purpose |
| --- | --- |
| `sparkless[dev]` | Development/tests: pytest, pandas, PyArrow, Polars, pytest-xdist, and other test tools |
| `sparkless[pyspark]` | PySpark 3.5 and its Delta integration; requires a compatible Java runtime |
| `sparkless[pyspark4]` | PySpark 4 and its Delta integration; requires a compatible Java runtime |
| `sparkless[server]` | The exactly matching `sparkless-server-bin` distribution |
| `sparkless[jdbc]` | Database integration test tools, testcontainers, and database drivers |

For example:

```bash
python -m pip install "sparkless[dev]>=4,<5"
```

Use separate environments for `pyspark` and `pyspark4`; they require different
Spark versions. The `jdbc` extra supplies test dependencies rather than enabling
every native database backend. Backend support and options depend on the build
and connection configuration.

### From source (contributors)

Use the [contributor setup](https://github.com/eddiethedean/robin-sparkless/blob/main/CONTRIBUTING.md#setup)
to create the repository-root uv environment, install development dependencies,
and build the native extension with Maturin. Rebuild after changing Rust or PyO3
code; an editable Python install alone does not rebuild the native library.

## Quick Start

### Basic Example

```python
from sparkless.sql import SparkSession, functions as F

# Create session
spark = SparkSession.builder.app_name("MyApp").get_or_create()

# Create DataFrame
data = [
    {"id": 1, "name": "Alice", "age": 25},
    {"id": 2, "name": "Bob", "age": 30},
]
df = spark.createDataFrame(data)

# Operations work like PySpark
result = df.filter(F.col("age") > 25).select("name")
print(result.collect())

df.show()
spark.stop()
```

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

The `sparkless.testing` module provides a unified framework for writing tests that run against both sparkless and PySpark backends.

### Quick Setup

Add to your `conftest.py`:

```python
pytest_plugins = ["sparkless.testing.plugin"]
```

### Unit Test Example

```python
def test_data_transformation(spark, spark_imports):
    """Test DataFrame logic against sparkless or PySpark."""
    F = spark_imports.F
    
    data = [{"value": 10}, {"value": 20}, {"value": 30}]
    df = spark.createDataFrame(data)

    result = df.filter(F.col("value") > 15).orderBy("value")

    assert result.count() == 2
    rows = result.collect()
    assert rows[0]["value"] == 20
    assert rows[1]["value"] == 30
```

### Running Tests

```bash
# Fast local tests (sparkless backend)
pytest tests -v

# Validate against PySpark (requires Java)
SPARKLESS_TEST_MODE=pyspark pytest tests -v
```

### Key Features

- **Fixtures:** `spark`, `spark_mode`, `spark_imports`, `isolated_session`, `table_prefix`
- **Markers:** `@pytest.mark.sparkless_only`, `@pytest.mark.pyspark_only`
- **Comparison utilities:** `assert_dataframes_equal()`, `compare_dataframes()`

For the complete guide, see [Testing Guide](TESTING_GUIDE.md).

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

The `server` extra installs a native executable. `SparklessServer` and
`AsyncSparklessServer` own its startup, readiness, and shutdown. Follow the
[Spark Connect guide](SPARK_CONNECT.md) for the pinned 4.18 client example,
HTTP-only operation, supported platforms, and the exact remote API/type limits.
The remote contract is narrower than the in-process Python API on this page.

## Performance

Workload size, query plans, startup costs, and the host machine affect runtime.
Measure representative workloads in your own environment; this guide does not
promise a fixed speedup over Spark. `collect()` materializes the complete result
in Python, and bounded Arrow batches do not bound total query memory.

## Next Steps

- [Testing Guide](TESTING_GUIDE.md) — Full guide to `sparkless.testing` (dual-mode testing, fixtures, comparison utilities)
- [Python API reference](PYTHON_API_REFERENCE.md) — API overview and Arrow export
- [Spark Connect](SPARK_CONNECT.md) — Native server and remote API contract
- [PySpark differences](PYSPARK_DIFFERENCES.md) — Known divergences and caveats
- [Migration (PySpark / Sparkless 3)](python_migration.md) — Switching from PySpark or Sparkless 3.x
- [Parity status](PARITY_STATUS.md) — Coverage and fixture status

## Getting Help

- **Repository:** [github.com/eddiethedean/robin-sparkless](https://github.com/eddiethedean/robin-sparkless)
- **Issues:** [github.com/eddiethedean/robin-sparkless/issues](https://github.com/eddiethedean/robin-sparkless/issues)
- **Sparkless 3.x (Polars Python):** [github.com/eddiethedean/sparkless](https://github.com/eddiethedean/sparkless)
