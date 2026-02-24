# sparkless

PySpark-like DataFrame API in Python—no JVM. Uses [robin-sparkless](https://github.com/eddiethedean/robin-sparkless) (Rust/Polars) as the execution engine.

**Version:** 4.0.0

## Installation

From the repo root:

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

## Quick start

```python
from sparkless.sql import SparkSession
from sparkless.sql.functions import col, lit_i64

spark = SparkSession.builder.app_name("demo").get_or_create()
df = spark.createDataFrame(
    [(1, 25, "Alice"), (2, 30, "Bob"), (3, 35, "Charlie")],
    ["id", "age", "name"],
)
adults = df.filter(col("age").gt(lit_i64(26)))
adults.show(10)
```

Use `SparkSession.builder` or `SparkSession.builder()`; `create_dataframe` / `createDataFrame(data, schema)`; and `col("x") > 1` for PySpark-style expressions.

## API

- **SparkSession**: `builder` / `builder()`, `config`, `read`, `create_dataframe`, `createDataFrame`, `table`, `sql`, `range`, `read_csv`, `read_parquet`, `read_json`, `read_delta`, `read_delta_with_version`, temp/global temp views, `catalog()`, `stop`.
- **DataFrame**: `filter`, `select`, `with_column`, `show`, `collect`, `count`, `group_by`, `order_by`, `limit`, `drop`, `distinct`, `join`, `union`, `union_all`, `write`, `create_or_replace_temp_view`, `columns`.
- **Column**: `alias`, `gt`, `ge`, `lt`, `le`, `eq`, `is_null`, `is_not_null`, `asc`, `desc`, `upper`, `lower`, `substr`, `length`, `trim`, `cast`; and `col("x") > 1`, `col("x") == 2` etc.
- **GroupedData**: `count`, `sum`, `avg`, `min`, `max`, `agg`, `pivot`.
- **PivotedGroupedData**: `sum`, `avg`, `min`, `max`, `count` (by value column).
- **DataFrameReader**: `option`, `options`, `format`, `load`, `csv`, `parquet`, `json`, `table`, `delta`.
- **DataFrameWriter**: `mode`, `option`, `partition_by`, `parquet`, `csv`, `json`, `save`, `save_as_table`.
- **Catalog**: `listTables()`, `listDatabases()`, `dropTempView(name)`.
- **Functions**: `col`, `lit`, `lit_i64`, `lit_str`, `lit_bool`, `lit_f64`, `lit_null`, `when`/`then`/`otherwise`, `upper`, `lower`, `substring`, `trim`, `cast`, `count`, `sum`, `avg`, `min`, `max`.

## UDFs

Rust UDFs can be registered in the engine (see the main repo’s [UDF guide](../docs/UDF_GUIDE.md)); the Python package does not yet expose a `register_udf` that accepts a Python callable. Use expressions and built-in functions from `sparkless.sql.functions` for now.

## Packaging

`maturin build` in the `python/` directory produces abi3 wheels for manylinux, macOS, and Windows. Use `requires-python = ">=3.9"` (or as set in `pyproject.toml`). Optional dev deps: `pip install -e ".[dev]"` for pytest and pandas.

Known differences from PySpark are documented in the main repo: [docs/PYSPARK_DIFFERENCES.md](../docs/PYSPARK_DIFFERENCES.md).
