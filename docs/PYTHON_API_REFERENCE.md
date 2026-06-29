# Python API Reference (Starter)

This is a **starter index** for the `sparkless` Python package. Full generated API docs are planned; until then use this page, [python/README.md](https://github.com/eddiethedean/robin-sparkless/blob/main/python/README.md), and type stubs in `python/sparkless/sparkless/_native.pyi`.

```python
from sparkless.sql import SparkSession, functions as F, types as T
```

## SparkSession

| Method / property | Description |
|-------------------|-------------|
| `SparkSession.builder.app_name(...).get_or_create()` | Create or reuse session |
| `createDataFrame(data, schema=None)` | Build DataFrame from rows, dicts, or tuples |
| `read` | `DataFrameReader` (csv, parquet, json, jdbc, …) |
| `sql(query)` | Run SQL (when SQL feature enabled) |
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
| `collect`, `count`, `show`, `toPandas` | Actions |
| `write` | `DataFrameWriter` (parquet, csv, json, jdbc, saveAsTable) |
| `createOrReplaceTempView` | Register for SQL |

## GroupedData

`count`, `sum`, `avg`, `min`, `max`, `agg`, `pivot`

## functions (`F`)

Common entry points (not exhaustive — see `sparkless.sql.functions`):

| Category | Examples |
|----------|----------|
| **Core** | `col`, `lit`, `when`, `coalesce`, `cast`, `alias` |
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
