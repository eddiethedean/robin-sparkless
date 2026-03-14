# Sparkless

**Test PySpark code at lightning speed—no JVM required**

Python 3.8+ · PySpark 3.2–3.5 compatible · License: MIT

**Current major release:** v4

_No JVM · PySpark-like API · Rust/Polars engine · Fast local tests_

---

## Sparkless v3 vs v4

| Version   | Backend              | Where it lives |
| --------- | -------------------- | -------------- |
| **Sparkless v3** | [Polars](https://pola.rs/) **Python** package | [github.com/eddiethedean/sparkless](https://github.com/eddiethedean/sparkless) — pure Python, `pip install sparkless` |
| **Sparkless v4** | **Rust** crate ([robin-sparkless](https://github.com/eddiethedean/robin-sparkless)) using Polars in Rust | This repo — Python API + native extension; `pip install ./python` |

**v4** keeps the same PySpark-like API but swaps the execution engine from Polars (Python) to **robin-sparkless** (Rust). You get a single native extension that talks to the Rust engine—no Polars Python dependency at runtime, and the same `from sparkless.sql import SparkSession` usage.

---

## Why Sparkless?

**Tired of waiting for Spark to initialize in every test?**

This package (Sparkless v4) provides a PySpark-like API that runs **without the JVM**, using the [robin-sparkless](https://github.com/eddiethedean/robin-sparkless) Rust crate as the execution engine. Use the same patterns as PySpark—just swap the import.

```python
# Before (PySpark)
from pyspark.sql import SparkSession

# After (Sparkless)
from sparkless.sql import SparkSession
```

### Key Benefits

| Feature                  | Description                                                |
| ------------------------ | ---------------------------------------------------------- |
| **No JVM**               | Pure Python + native extension; no Java required           |
| **PySpark-like API**     | `SparkSession`, `DataFrame`, `Column`, `functions as F`     |
| **Fast**                 | Polars-backed execution; no cluster or 30s startup          |
| **Same code**             | Write tests and pipelines that work with PySpark or here   |
| **Optional SQL**         | `spark.sql()`, temp views, `createOrReplaceTempView`       |
| **Delta & IO**           | Read/write Parquet, CSV, JSON, Delta when enabled          |

### Perfect For

- **Unit testing** — Fast, isolated runs without Spark infrastructure
- **CI/CD** — Reliable tests without JVM or resource leaks
- **Local development** — Prototype and iterate without a cluster
- **Learning** — PySpark-style API without setup complexity

---

## Installation

### From PyPI (recommended)

Install the published `sparkless` package:

```bash
pip install "sparkless>=4,<5"
```

This installs the Python API and the prebuilt native extension wheels (when available for your platform).

### From this repo (developing against robin-sparkless)

From the repo root (after cloning [robin-sparkless](https://github.com/eddiethedean/robin-sparkless)):

```bash
pip install ./python
```

Or from this directory:

```bash
pip install .
```

For development (editable install with the native extension):

```bash
maturin develop
```

---

## Quick Start

### Basic Usage

```python
from sparkless.sql import SparkSession, functions as F

# Create session
spark = SparkSession.builder.app_name("MyApp").get_or_create()

# Your PySpark-style code
data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
df = spark.createDataFrame(data)

# Filter and select
result = df.filter(F.col("age") > 25).select("name").collect()
print(result)  # [Row(name='Bob')]

# Show the DataFrame
df.show()
```

### Testing Example

```python
import pytest
from sparkless.sql import SparkSession, functions as F

def test_data_pipeline():
    """Test PySpark logic without a Spark cluster."""
    spark = SparkSession.builder.app_name("TestApp").get_or_create()

    data = [{"score": 95}, {"score": 87}, {"score": 92}]
    df = spark.createDataFrame(data)

    high_scores = df.filter(F.col("score") > 90)

    assert high_scores.count() == 2
    assert high_scores.agg(F.avg("score")).collect()[0][0] == 93.5

    spark.stop()
```

---

## Core Features

### PySpark-Compatible API

| Area           | What’s included                                                                 |
| -------------- | ------------------------------------------------------------------------------- |
| **SparkSession** | `builder`, `config`, `createDataFrame`, `create_dataframe`, `table`, `sql`, `range`, `read_csv`, `read_parquet`, `read_json`, `read_delta`, temp/global temp views, `catalog`, `stop` |
| **DataFrame**  | `filter`, `select`, `with_column` / `withColumn`, `show`, `collect`, `count`, `group_by`, `order_by`, `limit`, `drop`, `distinct`, `join`, `union`, `union_all`, `write`, `create_or_replace_temp_view`, `columns` |
| **Column**     | `alias`, `gt`, `ge`, `lt`, `le`, `eq`, `is_null`, `is_not_null`, `asc`, `desc`, `upper`, `lower`, `substr`, `length`, `trim`, `cast`; and `col("x") > 1`, `col("x") == 2` |
| **GroupedData** | `count`, `sum`, `avg`, `min`, `max`, `agg`, `pivot`                            |
| **Reader/Writer** | `option`, `options`, `format`, `load`, `parquet`, `csv`, `json`, `table`, `delta`, `jdbc`; `mode`, `partition_by`, `save`, `save_as_table` |
| **JDBC** | Read/write external databases: `spark.read.jdbc(url, table, properties)`, `df.write.jdbc(...)`. Supports PostgreSQL, SQLite, MySQL, MariaDB, SQL Server, Oracle, DB2. PySpark-compatible options: `sessionInitStatement`, `queryTimeout`, `batchsize`, `truncate`, all save modes |
| **Functions**  | `col`, `lit`, `when`/`then`/`otherwise`, `upper`, `lower`, `substring`, `trim`, `cast`, `count`, `sum`, `avg`, `min`, `max`, and many more in `sparkless.sql.functions` |

Full parity status and known differences from PySpark are documented in the main repo: [PYSPARK_DIFFERENCES.md](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PYSPARK_DIFFERENCES.md) and [PARITY_STATUS.md](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PARITY_STATUS.md).

### SQL Support

```python
df = spark.createDataFrame([{"name": "Alice", "salary": 50000}, {"name": "Bob", "salary": 60000}])
df.createOrReplaceTempView("employees")

result = spark.sql("SELECT name, salary FROM employees WHERE salary > 50000")
result.show()
```

### JDBC / Database Support

Read from and write to external databases using PySpark-compatible JDBC API:

```python
# Read from PostgreSQL
url = "jdbc:postgresql://localhost:5432/mydb"
props = {"user": "admin", "password": "secret"}
df = spark.read.jdbc(url=url, table="users", properties=props)

# Or use the format API with options
df = (spark.read
    .format("jdbc")
    .option("url", url)
    .option("dbtable", "users")
    .option("sessionInitStatement", "SET timezone='UTC'")
    .options(props)
    .load("."))

# Write back with batching
df.write.jdbc(
    url=url,
    table="users_backup",
    properties={"batchsize": "5000", **props},
    mode="overwrite"
)

# SQLite (file-based, no server required)
df = spark.read.jdbc(url="jdbc:sqlite:/path/to/db.sqlite", table="my_table", properties={})
```

Supported databases: PostgreSQL, SQLite, MySQL, MariaDB, SQL Server, Oracle, DB2.

### Lazy Evaluation

Transformations are queued; execution happens on actions (`collect()`, `count()`, `show()`), matching PySpark’s model.

---

## Backend (v4)

Sparkless v4 is the **Python face** of the [robin-sparkless](https://github.com/eddiethedean/robin-sparkless) Rust crate. The Python package wraps a native extension (`sparkless._native`) built from the **sparkless-native** crate, which calls into **robin-sparkless** (Polars in Rust). There is no JVM and no Polars Python dependency—execution is entirely in the Rust engine.

---

## Testing with sparkless.testing (new in v4.4)

The `sparkless.testing` module provides a unified framework for writing tests that run against both sparkless and PySpark backends.

### Quick Setup

```python
# conftest.py
pytest_plugins = ["sparkless.testing"]
```

### Write Tests

```python
def test_filter(spark):
    df = spark.createDataFrame([{"id": 1}, {"id": 2}])
    assert df.filter(df.id > 1).count() == 1

def test_with_functions(spark, spark_imports):
    F = spark_imports.F
    df = spark.createDataFrame([{"name": "alice"}])
    result = df.select(F.upper("name")).collect()
    assert result[0][0] == "ALICE"
```

### Run Tests

```bash
# Fast local tests (sparkless backend)
pytest tests/ -v

# Validate against PySpark
SPARKLESS_TEST_MODE=pyspark pytest tests/ -v
```

### Delta Lake Support (v4.4.1+)

Enable Delta Lake for PySpark mode using the `@pytest.mark.delta` marker or environment variable:

```python
import pytest

@pytest.mark.delta
def test_delta_table(spark, spark_imports):
    """This test has Delta Lake enabled in PySpark mode."""
    F = spark_imports.F
    df = spark.createDataFrame([{"id": 1, "value": 100}])
    
    # Delta operations work in PySpark mode
    df.write.format("delta").mode("overwrite").save("/tmp/my_delta_table")
    
    result = spark.read.format("delta").load("/tmp/my_delta_table")
    assert result.count() == 1
```

Or enable Delta Lake for all tests:

```bash
SPARKLESS_ENABLE_DELTA=1 SPARKLESS_TEST_MODE=pyspark pytest tests/ -v
```

### Features

- **Fixtures:** `spark`, `spark_mode`, `spark_imports`, `isolated_session`, `table_prefix`
- **Markers:** `@pytest.mark.sparkless_only`, `@pytest.mark.pyspark_only`, `@pytest.mark.delta`
- **Comparison:** `assert_dataframes_equal()`, `compare_dataframes()`
- **Environment Variables:**
  - `SPARKLESS_TEST_MODE` — `sparkless` (default) or `pyspark`
  - `SPARKLESS_ENABLE_DELTA` — Enable Delta Lake in PySpark mode (`1`, `true`, `yes`)

See the full [Testing Guide](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/TESTING_GUIDE.md) for complete documentation.

---

## Development

```bash
# Editable install with native extension
maturin develop

# Run tests (from repo root)
pytest tests -v

# With PySpark backend (requires Java + pyspark)
SPARKLESS_TEST_MODE=pyspark pytest tests -v
```

Optional dev dependencies:

```bash
pip install -e "./python[dev]"
```

Packaging: `maturin build` in the `python/` directory produces wheels. See the main repo [README](https://github.com/eddiethedean/robin-sparkless) and [docs](https://github.com/eddiethedean/robin-sparkless/tree/main/docs) for Rust development, UDFs, and embedding.

---

## Known Limitations

- **UDFs:** Python callable UDFs are not yet exposed; use built-in functions and expressions. Rust UDFs can be registered in the engine (see [UDF_GUIDE.md](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/UDF_GUIDE.md)).
- **Compatibility:** Some PySpark APIs or behaviors may differ; see [PYSPARK_DIFFERENCES.md](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PYSPARK_DIFFERENCES.md).

---

## License

MIT — see [LICENSE](https://github.com/eddiethedean/robin-sparkless/blob/main/LICENSE) in the main repository.

---

## Links

- **Repository (this package):** [github.com/eddiethedean/robin-sparkless](https://github.com/eddiethedean/robin-sparkless) — Rust engine + Python bindings
- **Sparkless v3 (Polars Python backend):** [github.com/eddiethedean/sparkless](https://github.com/eddiethedean/sparkless)
- **Documentation:** [robin-sparkless.readthedocs.io](https://robin-sparkless.readthedocs.io/)
