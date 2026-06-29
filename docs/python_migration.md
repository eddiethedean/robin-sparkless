# Migration Guide (Python — Sparkless v4)

This page helps you switch to **Sparkless v4** (Rust backend) from **PySpark** or from **Sparkless 3.x** (Polars Python backend).

## Sparkless 3.x → v4

| Aspect        | Sparkless 3.x              | Sparkless v4                    |
| ------------- | -------------------------- | --------------------------------- |
| **Backend**   | Polars **Python** package   | **Rust** crate (robin-sparkless)  |
| **Install**   | `pip install sparkless` (3.x) | `pip install "sparkless>=4,<5"` (PyPI) |
| **Import**    | `from sparkless.sql import SparkSession` | Same |
| **API**       | PySpark-like               | Same PySpark-like API             |
| **Runtime**  | Polars + Python            | Native extension + Rust; no Polars Python |

**What to do:**

1. Install Sparkless v4 from **PyPI**: `pip install "sparkless>=4,<5"`. Contributors can use `pip install ./python` from the [robin-sparkless](https://github.com/eddiethedean/robin-sparkless) repo.
2. Keep your existing `from sparkless.sql import SparkSession, functions as F` and DataFrame code.
3. Use `SparkSession.builder.app_name("...").get_or_create()` if you already do; v4 supports the same builder API.
4. See [PySpark differences](PYSPARK_DIFFERENCES.md) for any behavioral differences from PySpark (and thus from Sparkless 3.x where it matches PySpark).

No code changes are required for typical tests and pipelines; the main difference is the execution engine (Rust instead of Polars Python).

## Targeting PySpark 4 (opt-in, 4.9.0+)

Sparkless **4.9.0** ships PySpark 4 semantics as an **opt-in profile**. The default remains PySpark 3.5-like (`compat=3.5`, ANSI off).

```python
from sparkless.sql import SparkSession

spark = SparkSession.builder.app_name("app").get_or_create()
spark.conf.set("sparkless.pyspark.compat", "4.0")  # enables ANSI + 4.0 map/collect rules
```

Or set the environment variable before session creation:

```bash
export SPARKLESS_PYSPARK_COMPAT=4.0
```

See [PYSPARK_COMPAT_PROFILES.md](PYSPARK_COMPAT_PROFILES.md) for bundled config keys and [PYSPARK_4_PARITY_PLAN.md](PYSPARK_4_PARITY_PLAN.md) for the full roadmap.

### Dual-oracle testing (PySpark 3.5 + 4.1)

| Oracle | Requirements | Env |
|--------|--------------|-----|
| PySpark 3.5 (default CI) | `tests/requirements-pyspark.txt` | `SPARKLESS_TEST_MODE=pyspark` |
| PySpark 4.1 (nightly) | `tests/requirements-pyspark4.txt`, Java 17, Python 3.10+ | `SPARKLESS_TEST_MODE=pyspark` + `SPARKLESS_PYSPARK_COMPAT=4.0` |

Use `@pytest.mark.pyspark4_only` / `pyspark3_only` for profile-specific tests. The `spark` fixture applies `sparkless.pyspark.compat` automatically when running against the sparkless backend.

## PySpark → Sparkless v4

### Quick swap

```python
# Before (PySpark)
from pyspark.sql import SparkSession, functions as F

# After (Sparkless v4)
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

You can run the same test suite against PySpark or Sparkless v4:

```bash
# Sparkless v4 (default in this repo)
pytest tests -v

# Real PySpark (requires Java + pyspark)
SPARKLESS_TEST_BACKEND=pyspark pytest tests -v
```

## See also

- [Getting started (Python)](python_getting_started.md)
- [PySpark differences](PYSPARK_DIFFERENCES.md)
- [Package README](https://github.com/eddiethedean/robin-sparkless/blob/main/python/README.md) — Sparkless 3 vs 4.x, installation, API overview
