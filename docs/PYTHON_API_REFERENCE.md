# Python API Reference

This page summarizes the in-process `sparkless` Python API. Start with
[getting started](python_getting_started.md) for installation and runnable
examples. Spark Connect has its own [supported request contract](SPARK_CONNECT.md);
local API support does not imply remote support.

```python
from sparkless.sql import SparkSession, functions as F, types as T
```

## SparkSession

| Method / property | Description |
|-------------------|-------------|
| `SparkSession.builder.app_name(...).get_or_create()` | Create or reuse session |
| `createDataFrame(data, schema=None)` | Build DataFrame from rows, dicts, or tuples |
| `read` | `DataFrameReader` (csv, parquet, json, jdbc, …) |
| `sql(query)` | Run SQL (enabled in the official Python build) |
| `table(name)` | Read temp or saved table |
| `catalog()` | List/drop tables and views |
| `conf` | Session configuration |
| `stop()` | Stop session |

## DataFrame

| Method | Description |
|--------|-------------|
| `filter` / `where` | Row filter |
| `select`, `selectExpr` | Project columns |
| `withColumn`, `withColumns`, `withColumnRenamed` | Add/rename columns |
| `groupBy` | Group for aggregations |
| `join` | Join with another DataFrame |
| `orderBy` / `sort` | Sort rows |
| `limit`, `distinct`, `drop`, `union`, `unionByName` | Standard transforms |
| `collect`, `count`, `show`, `toPandas`, `toArrow` | Actions; collect/export methods materialize results |
| `toArrowBatches` | Bounded Arrow RecordBatch iterator; see below |
| `write` | `DataFrameWriter` (parquet, csv, json, jdbc, saveAsTable) |
| `createOrReplaceTempView` | Register for SQL |

## Arrow export

Install PyArrow with `python -m pip install pyarrow`. `toArrow()` returns a
materialized PyArrow Table. `toArrowBatches()` produces RecordBatches from the
query plan with explicit row and byte limits:

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

Each batch respects both limits. `max_bytes` is checked against
`RecordBatch.nbytes`; a row larger than that value raises `ValueError`. Defaults
are 10,000 rows and 64 MiB per batch. These are batch limits, not a process-memory
or query-intermediate limit.

Use the iterator as a context manager, especially when breaking out early, so
closing it stops further batch production. `collect()` and `toArrow()` still
materialize the complete result.

## GroupedData

`count`, `sum`, `avg`, `min`, `max`, `agg`, `pivot`

## functions (`F`)

Common entry points (not exhaustive — see `sparkless.sql.functions`):

| Category | Examples |
|----------|----------|
| **Core** | `col`, `lit`, `when`, `coalesce`, `cast` |
| **Aggregates** | `count`, `sum`, `avg`, `min`, `max`, `countDistinct`, `collect_list`, `collect_set` |
| **String** | `upper`, `lower`, `trim`, `substring`, `regexp_replace`, `split`, `concat`, `length` |
| **Datetime** | `current_date`, `current_timestamp`, `to_date`, `date_format`, `datediff`, `date_add` |
| **Array** | `explode`, `array_contains`, `size`, `array_compact` |
| **Window** | `row_number`, `rank`, `dense_rank`, `lag`, `lead` (with `.over(window)`) |
| **Null** | `isnull`, `isnan`, `nvl`, `nanvl` |

## types (`T`)

`StringType`, `IntegerType`, `LongType`, `DoubleType`, `BooleanType`, `DateType`, `TimestampType`, `ArrayType`, `MapType`, `StructType`, `StructField`, `Row`

## Testing

```python
from sparkless.testing import assert_dataframes_equal  # see TESTING_GUIDE.md
```

## Parity and gaps

Not every PySpark API is implemented. See [PySpark differences](PYSPARK_DIFFERENCES.md) and [Deferred scope](DEFERRED_SCOPE.md).

Native bindings: [`_native.pyi`](https://github.com/eddiethedean/robin-sparkless/blob/main/python/sparkless/sparkless/_native.pyi)
