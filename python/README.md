# Sparkless

**PySpark-compatible DataFrames in Python—no JVM, no cluster.**  
Drop-in replacement for local development and testing. Same API, 10–100× faster feedback.

- **Python 3.8+** · **PySpark 3.2–3.5 compatible** · MIT

---

## In 30 seconds

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

No Java, no Spark install, no 30-second session startup. Ideal for unit tests and CI.

---

## Why Sparkless?

| If you… | Sparkless gives you… |
|--------|------------------------|
| **Test PySpark code** | Same `SparkSession` / `DataFrame` / `Column` API; tests run in seconds without a JVM |
| **Run in CI** | No Java, no Spark distribution, no flaky cluster; just `pip install` and `pytest` |
| **Develop locally** | Fast iteration with Parquet, CSV, JSON, Delta, SQL, temp views—no cluster needed |
| **Need real PySpark sometimes** | Use `sparkless.testing`: one test suite, run with `sparkless` or `SPARKLESS_TEST_MODE=pyspark` |

**v4** uses the [robin-sparkless](https://github.com/eddiethedean/robin-sparkless) Rust engine (Polars under the hood). No Polars Python dependency at runtime—just a small native extension.

---

## Installation

**From PyPI (recommended)**

```bash
pip install "sparkless>=4,<5"
```

Prebuilt wheels are provided for common platforms (Linux, macOS, Windows).  
For **development** from the [robin-sparkless](https://github.com/eddiethedean/robin-sparkless) repo:

```bash
# From repo root
pip install ./python

# Or editable install (rebuilds native code on changes)
cd python && maturin develop
```

**Optional extras**

| Extra | Use case |
|-------|----------|
| `pip install "sparkless[dev]"` | pytest, pandas, hypothesis, pytest-xdist |
| `pip install "sparkless[jdbc]"` | JDBC integration tests (testcontainers, drivers) |
| `pip install "sparkless[pyspark]"` | Run tests against real PySpark (`SPARKLESS_TEST_MODE=pyspark`) |

---

## Quick start

### Basic usage

```python
from sparkless.sql import SparkSession, functions as F

spark = SparkSession.builder.app_name("MyApp").get_or_create()
data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
df = spark.createDataFrame(data)

df.filter(F.col("age") > 25).select("name").show()
# +----+
# |name|
# +----+
# | Bob|
# +----+

result = df.filter(F.col("age") > 25).select("name").collect()
# [Row(name='Bob')]
```

### In tests (pytest)

```python
import pytest
from sparkless.sql import SparkSession, functions as F

def test_high_scores():
    spark = SparkSession.builder.app_name("test").get_or_create()
    df = spark.createDataFrame([{"score": 95}, {"score": 87}, {"score": 92}])
    high = df.filter(F.col("score") > 90)
    assert high.count() == 2
    assert high.agg(F.avg("score")).collect()[0][0] == 93.5
    spark.stop()
```

### SQL and temp views

```python
df = spark.createDataFrame([{"name": "Alice", "salary": 50000}, {"name": "Bob", "salary": 60000}])
df.createOrReplaceTempView("employees")

spark.sql("SELECT name, salary FROM employees WHERE salary > 50000").show()
```

### JDBC (PostgreSQL, SQLite, MySQL, etc.)

```python
# Read
url = "jdbc:postgresql://localhost:5432/mydb"
df = spark.read.jdbc(url=url, table="users", properties={"user": "u", "password": "p"})

# Write
df.write.jdbc(url=url, table="users_backup", properties={"user": "u", "password": "p"}, mode="overwrite")
```

Supported: PostgreSQL, SQLite, MySQL, MariaDB, SQL Server, Oracle, DB2. Same options as PySpark (`sessionInitStatement`, `batchsize`, `truncate`, etc.).

---

## API at a glance

| Area | Supported |
|------|-----------|
| **SparkSession** | `builder`, `createDataFrame`, `table`, `sql`, `range`, `read_csv` / `read_parquet` / `read_json` / `read_delta`, temp/global temp views, `catalog`, `stop` |
| **DataFrame** | `filter`, `select`, `withColumn`, `show`, `collect`, `count`, `groupBy`, `orderBy`, `limit`, `drop`, `distinct`, `join`, `union`, `unionByName`, `write`, `createOrReplaceTempView` |
| **Column / functions** | `col`, `lit`, `when`/`then`/`otherwise`, comparison/aggregation/string/window functions; `F.col("x") > 1`, `F.upper("name")`, etc. |
| **GroupedData** | `count`, `sum`, `avg`, `min`, `max`, `agg`, `pivot` |
| **Read/Write** | Parquet, CSV, JSON, Delta; `saveAsTable`; JDBC with standard options |

Detailed parity and differences: [PYSPARK_DIFFERENCES.md](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PYSPARK_DIFFERENCES.md), [PARITY_STATUS.md](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PARITY_STATUS.md).

---

## Dual-mode testing (sparkless + PySpark)

Use **sparkless.testing** to run the same tests with the sparkless backend (fast) or real PySpark (parity check).

**1. Register the plugin** (e.g. in `conftest.py`):

```python
pytest_plugins = ["sparkless.testing"]
```

**2. Write tests** using the `spark` and optional `spark_imports` fixtures:

```python
def test_filter(spark):
    df = spark.createDataFrame([{"id": 1}, {"id": 2}])
    assert df.filter(df.id > 1).count() == 1

def test_upper(spark, spark_imports):
    F = spark_imports.F
    df = spark.createDataFrame([{"name": "alice"}])
    assert df.select(F.upper("name")).collect()[0][0] == "ALICE"
```

**3. Run**

```bash
# Default: sparkless backend (no JVM)
pytest tests/ -v

# Parallel
pytest tests/ -n 12 -v

# Against real PySpark (requires Java + pip install pyspark)
SPARKLESS_TEST_MODE=pyspark pytest tests/ -v
```

**Fixtures:** `spark`, `spark_mode`, `spark_imports`, `isolated_session`, `table_prefix`  
**Markers:** `@pytest.mark.sparkless_only`, `@pytest.mark.pyspark_only`, `@pytest.mark.delta`  
**Helpers:** `assert_dataframes_equal()`, `compare_dataframes()`

Full details: [TESTING_GUIDE.md](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/TESTING_GUIDE.md).

---

## Requirements

- **Python 3.8+**
- **Sparkless mode:** No Java or Spark required.
- **PySpark mode** (optional): Java and `pip install pyspark` for `SPARKLESS_TEST_MODE=pyspark`.

---

## Development

```bash
cd python
maturin develop          # Editable install
pytest tests -v          # From repo root: run tests (sparkless backend)
SPARKLESS_TEST_MODE=pyspark pytest tests -v   # With PySpark
```

Package wheels: `maturin build` in `python/`.  
Rust engine, UDFs, embedding: see the main [README](https://github.com/eddiethedean/robin-sparkless) and [docs](https://github.com/eddiethedean/robin-sparkless/tree/main/docs).

---

## Limitations

- **UDFs:** Use built-in functions and expressions; Python UDFs are not exposed. Rust UDFs are supported in the engine ([UDF_GUIDE](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/UDF_GUIDE.md)).
- **Compatibility:** Some APIs or behaviors differ from PySpark; see [PYSPARK_DIFFERENCES.md](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PYSPARK_DIFFERENCES.md).

---

## Version history: v3 vs v4

| | Sparkless v3 | Sparkless v4 (this package) |
|--|--------------|-----------------------------|
| **Backend** | Polars (Python) | robin-sparkless (Rust + Polars) |
| **Repo** | [sparkless](https://github.com/eddiethedean/sparkless) | [robin-sparkless](https://github.com/eddiethedean/robin-sparkless) |
| **Install** | `pip install sparkless` | `pip install "sparkless>=4,<5"` or `pip install ./python` |
| **Runtime** | Polars Python | Native extension only; no Polars Python |

Same `from sparkless.sql import SparkSession` in both. v4 is the current major release.

---

## Links

- **This repo:** [github.com/eddiethedean/robin-sparkless](https://github.com/eddiethedean/robin-sparkless) — Rust engine + Python package
- **Docs:** [robin-sparkless.readthedocs.io](https://robin-sparkless.readthedocs.io/)
- **Sparkless v3:** [github.com/eddiethedean/sparkless](https://github.com/eddiethedean/sparkless)  
- **License:** MIT ([LICENSE](https://github.com/eddiethedean/robin-sparkless/blob/main/LICENSE) in main repo)
