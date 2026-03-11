# Getting Started (Python — Sparkless 4.1)

This guide is for the **Sparkless 4.1** Python package: a PySpark-like API backed by the [robin-sparkless](https://github.com/eddiethedean/robin-sparkless) Rust engine. Sparkless 3.x uses the Polars **Python** package; 4.x uses the **Rust** crate (no Polars Python at runtime).

## Installation

Install from the robin-sparkless repository (after cloning):

```bash
pip install ./python
```

Or from the `python/` directory:

```bash
pip install .
```

For development (editable install with the native extension):

```bash
cd python && maturin develop
```

Optional dependencies:

```bash
pip install -e ".[dev]"    # pytest, pandas, pytest-xdist, etc.
pip install -e ".[pyspark]" # run tests with real PySpark (requires Java)
```

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
print(result.collect())  # [Row(name='Bob')]

df.show()
spark.stop()
```

### Drop-in PySpark Replacement

Sparkless 4.1 is designed to be a drop-in replacement for PySpark in tests and local workflows:

```python
# Before (PySpark)
from pyspark.sql import SparkSession

# After (Sparkless 4.1)
from sparkless.sql import SparkSession
```

Use `SparkSession.builder.app_name("...").get_or_create()` or `SparkSession("AppName")`; the rest of your PySpark-style code can stay the same.

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
from sparkless.sql import Window, functions as F

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

## Testing with Sparkless 4.1

### Unit Test Example

```python
import pytest
from sparkless.sql import SparkSession, functions as F

def test_data_transformation():
    """Test DataFrame logic without a Spark cluster."""
    spark = SparkSession.builder.app_name("TestApp").get_or_create()

    data = [{"value": 10}, {"value": 20}, {"value": 30}]
    df = spark.createDataFrame(data)

    result = df.filter(F.col("value") > 15)

    assert result.count() == 2
    rows = result.collect()
    assert rows[0]["value"] == 20
    assert rows[1]["value"] == 30

    spark.stop()
```

Run tests from the repo root:

```bash
pytest tests -v
# With real PySpark (requires Java):
SPARKLESS_TEST_BACKEND=pyspark pytest tests -v
```

## Performance

Sparkless 4.1 uses the Rust engine (Polars in Rust). There is no JVM and no Polars Python dependency at runtime.

| Operation         | PySpark | Sparkless 4.1 |
| ----------------- | ------- | ------------- |
| Session creation  | 30–45s  | &lt; 1s       |
| Simple query      | 2–5s    | &lt; 0.1s     |
| Full test suite   | 5–10 min| 1–2 min       |

## Next Steps

- [Package README](https://github.com/eddiethedean/robin-sparkless/blob/main/python/README.md) — Installation, Sparkless 3 vs 4.x, API overview, backend
- [PySpark differences](PYSPARK_DIFFERENCES.md) — Known divergences and caveats
- [Migration (PySpark / Sparkless 3)](python_migration.md) — Switching from PySpark or Sparkless 3.x
- [Parity status](PARITY_STATUS.md) — Coverage and fixture status

## Getting Help

- **Repository:** [github.com/eddiethedean/robin-sparkless](https://github.com/eddiethedean/robin-sparkless)
- **Issues:** [github.com/eddiethedean/robin-sparkless/issues](https://github.com/eddiethedean/robin-sparkless/issues)
- **Sparkless 3.x (Polars Python):** [github.com/eddiethedean/sparkless](https://github.com/eddiethedean/sparkless)
