# Sparkless

**Test PySpark code at lightning speed—no JVM required**

Python 3.8+ · PySpark 3.2–3.5 compatible · License: MIT

**Current major release:** v4

_No JVM · PySpark-like API · Rust/Polars engine · Fast local tests_

---

## Sparkless 3 vs 4.x

| Version   | Backend              | Where it lives |
| --------- | -------------------- | -------------- |
| **Sparkless 3.x** | [Polars](https://pola.rs/) **Python** package | [github.com/eddiethedean/sparkless](https://github.com/eddiethedean/sparkless) — pure Python, `pip install sparkless` |
| **Sparkless 4.x** | **Rust** crate ([robin-sparkless](https://github.com/eddiethedean/robin-sparkless)) using Polars in Rust | This repo — Python API + native extension; `pip install ./python` |

**4.x** keeps the same PySpark-like API but swaps the execution engine from Polars (Python) to **robin-sparkless** (Rust). You get a single native extension that talks to the Rust engine—no Polars Python dependency at runtime, and the same `from sparkless.sql import SparkSession` usage.

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
| **Reader/Writer** | `option`, `options`, `format`, `load`, `parquet`, `csv`, `json`, `table`, `delta`; `mode`, `partition_by`, `save`, `save_as_table` |
| **Functions**  | `col`, `lit`, `when`/`then`/`otherwise`, `upper`, `lower`, `substring`, `trim`, `cast`, `count`, `sum`, `avg`, `min`, `max`, and many more in `sparkless.sql.functions` |

Full parity status and known differences from PySpark are documented in the main repo: [PYSPARK_DIFFERENCES.md](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PYSPARK_DIFFERENCES.md) and [PARITY_STATUS.md](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PARITY_STATUS.md).

### SQL Support

```python
df = spark.createDataFrame([{"name": "Alice", "salary": 50000}, {"name": "Bob", "salary": 60000}])
df.createOrReplaceTempView("employees")

result = spark.sql("SELECT name, salary FROM employees WHERE salary > 50000")
result.show()
```

### Lazy Evaluation

Transformations are queued; execution happens on actions (`collect()`, `count()`, `show()`), matching PySpark’s model.

---

## Backend (4.x)

Sparkless 4.x is the **Python face** of the [robin-sparkless](https://github.com/eddiethedean/robin-sparkless) Rust crate. The Python package wraps a native extension (`sparkless._native`) built from the **sparkless-native** crate, which calls into **robin-sparkless** (Polars in Rust). There is no JVM and no Polars Python dependency—execution is entirely in the Rust engine.

---

## Development

```bash
# Editable install with native extension
maturin develop

# Run tests (from repo root)
pytest tests -v

# With PySpark backend (requires Java + pyspark)
SPARKLESS_TEST_BACKEND=pyspark pytest tests -v
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
- **Sparkless 3.x (Polars Python backend):** [github.com/eddiethedean/sparkless](https://github.com/eddiethedean/sparkless)
- **Documentation:** [robin-sparkless.readthedocs.io](https://robin-sparkless.readthedocs.io/)
