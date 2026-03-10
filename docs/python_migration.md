# Migration Guide (Python — Sparkless 4.0)

This page helps you switch to **Sparkless 4.0** (Rust backend) from **PySpark** or from **Sparkless 3.x** (Polars Python backend).

## Sparkless 3.x → 4.0

| Aspect        | Sparkless 3.x              | Sparkless 4.0                    |
| ------------- | -------------------------- | --------------------------------- |
| **Backend**   | Polars **Python** package   | **Rust** crate (robin-sparkless)  |
| **Install**   | `pip install sparkless`    | `pip install ./python` (this repo) |
| **Import**    | `from sparkless.sql import SparkSession` | Same |
| **API**       | PySpark-like               | Same PySpark-like API             |
| **Runtime**  | Polars + Python            | Native extension + Rust; no Polars Python |

**What to do:**

1. Install the 4.0 package from the [robin-sparkless](https://github.com/eddiethedean/robin-sparkless) repo: `pip install ./python` (or from a built wheel).
2. Keep your existing `from sparkless.sql import SparkSession, functions as F` and DataFrame code.
3. Use `SparkSession.builder.app_name("...").get_or_create()` if you already do; 4.0 supports the same builder API.
4. See [PySpark differences](PYSPARK_DIFFERENCES.md) for any behavioral differences from PySpark (and thus from Sparkless 3.x where it matches PySpark).

No code changes are required for typical tests and pipelines; the main difference is the execution engine (Rust instead of Polars Python).

## PySpark → Sparkless 4.0

### Quick swap

```python
# Before (PySpark)
from pyspark.sql import SparkSession, functions as F

# After (Sparkless 4.0)
from sparkless.sql import SparkSession, functions as F
```

Use `SparkSession.builder.app_name("MyApp").get_or_create()` or `SparkSession("MyApp")`. Your DataFrame operations, `F.col`, `filter`, `select`, `groupBy`, `join`, and SQL (`spark.sql`, `createOrReplaceTempView`) work the same way.

### Common patterns

- **Session:** `SparkSession.builder.app_name("App").get_or_create()` — same as PySpark.
- **DataFrames:** `createDataFrame(data)`, `createDataFrame(data, schema)`, `read_csv`, `read_parquet`, `read_json`, `read_delta` (when enabled).
- **Expressions:** `F.col("x")`, `F.lit(...)`, `F.when(...).otherwise(...)`, and built-in functions in `sparkless.sql.functions`.
- **SQL:** `df.createOrReplaceTempView("name")`, `spark.sql("SELECT ...")` (when the `sql` feature is enabled in the Rust build).

### What’s different or unsupported

See [PySpark differences](PYSPARK_DIFFERENCES.md) for the full list. Summary:

- **RDD, streaming, mapInPandas:** Not supported; use `collect()` or local patterns.
- **Some SQL/DDL:** Only a subset of statements (SELECT, temp views, saved tables, etc.); see PYSPARK_DIFFERENCES.
- **Join on expression:** Use column-name joins; expression joins (e.g. `df1.a == df2.b` in the join API) are not supported.
- **UDFs:** Python callable UDFs are not yet exposed; use built-in functions. Rust UDFs can be registered in the engine.

### Testing both backends

You can run the same test suite against PySpark or Sparkless 4.0:

```bash
# Sparkless 4.0 (default in this repo)
pytest tests -v

# Real PySpark (requires Java + pyspark)
SPARKLESS_TEST_BACKEND=pyspark pytest tests -v
```

## See also

- [Getting started (Python)](python_getting_started.md)
- [PySpark differences](PYSPARK_DIFFERENCES.md)
- [Package README](https://github.com/eddiethedean/robin-sparkless/blob/main/python/README.md) — Sparkless 3 vs 4.0, installation, API overview
