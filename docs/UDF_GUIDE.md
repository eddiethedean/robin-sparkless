# UDF Guide

Robin-sparkless supports **user-defined functions** (UDFs) in PySpark style:

- Scalar **Python UDFs** (row-at-a-time)
- Vectorized **Python UDFs** (pandas_udf-like; batch-at-a-time)
- **Rust UDFs** (lazy, Expr-based)

## Python UDFs (scalar)

### Registration

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("test").get_or_create()

def double(x):
    return x * 2 if x is not None else None

# Register with default return type (string) or explicit type
my_udf = spark.udf().register("double", double, return_type="int")
```

### Usage

Use the returned UDF as a column expression:

```python
df = spark.createDataFrame([(1, 10, "a"), (2, 20, "b")], ["id", "v", "name"])
df2 = df.with_column("doubled", my_udf(rs.col("id")))
# df2.collect() → [{'id': 1, 'v': 10, 'name': 'a', 'doubled': 2}, {'id': 2, 'v': 20, 'name': 'b', 'doubled': 4}]
```

Or call by name:

```python
df2 = df.with_column("d2", rs.call_udf("double", rs.col("id")))
```

### Return types

`return_type` can be omitted (defaults to `StringType`), a DDL string (`"int"`, `"bigint"`, `"string"`, `"double"`, `"boolean"`, `"date"`, `"timestamp"`), or a DataType-like object with `typeName`.

### Limitations

- **Row-at-a-time execution**: Scalar Python UDFs run row-by-row; data is materialized at the UDF boundary.
- **WHERE/HAVING**: Python UDFs (scalar or vectorized) in SQL WHERE or HAVING are not yet supported.
- **Plan interpreter / filter**: Python UDFs (scalar or vectorized) work in `withColumn` / `select` only.
  Using them in filter/join/groupBy plan expressions returns a clear error.

## Vectorized Python UDFs (pandas_udf-style, column-wise)

Vectorized UDFs operate on **column batches** instead of individual rows. They are registered with
`vectorized=True` and receive Python sequences (e.g. lists or pandas.Series) and must return one
value per input element.

### Registration (column-wise)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("vec_udf").get_or_create()

def double_vec(col):
    # col is a 1-D sequence of values (list, pandas.Series, etc.)
    return [x * 2 if x is not None else None for x in col]

my_udf = spark.udf().register(
    "double_vec",
    double_vec,
    return_type="int",
    vectorized=True,
)
```

### Usage from DataFrame

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("vec_udf").get_or_create()

# Single-column schema: use createDataFrame with explicit schema
df = spark.createDataFrame([{"id": 1}, {"id": 2}, {"id": 3}], [("id", "bigint")])

# Using the returned UserDefinedFunction
df2 = df.with_column("d2", my_udf(rs.col("id")))

# Using call_udf by name
df3 = df.with_column("d3", rs.call_udf("double_vec", rs.col("id")))
rows = df3.collect()
assert [r["d3"] for r in rows] == [2, 4, 6]
```

### Semantics and constraints

- **Input**: Each argument is passed as a Python sequence of values for the current batch
  (current implementation materializes the full column; future versions may stream in chunks).
- **Output length**: The UDF **must return the same number of elements** as the input column;
  otherwise an error is raised.
- **Nulls**: `None` values are preserved round-trip.
- **Supported contexts**: Column-wise vectorized UDFs are supported in:
  - `DataFrame.with_column(...)`
  - `DataFrame.select(...)` / `select_expr` via `call_udf`
- **Unsupported contexts**: Using column-wise vectorized UDFs in:
  - `filter` / `where`
  - join conditions
  - **groupBy aggregations** (use grouped vectorized UDFs instead; see below)
  - SQL WHERE/HAVING
  will raise a clear error: Python/Vectorized UDFs are only supported in withColumn/select paths.

## Grouped vectorized Python UDFs (pandas_udf-style GROUPED_AGG)

Grouped vectorized UDFs operate on **per-group batches** and return a **single value per group**.
They are intended to mirror PySpark ``pandas_udf(..., functionType=\"GROUPED_AGG\")`` semantics,
but are implemented in v1 using plain Python sequences (lists), not pandas Series/DataFrames.

### Registration

Use the module-level ``pandas_udf`` helper:

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("grouped").get_or_create()

@rs.pandas_udf("double", function_type="grouped_agg")
def mean_udf(values):
    # values is a Python sequence (list) of values for the current group
    if not values:
        return None
    return sum(values) / len(values)
```

Notes:

- **function_type**: Only ``"grouped_agg"`` is supported in v1.
- **return_type**: Required. Follows the same rules as scalar/vectorized UDFs
  (DDL string such as ``"int"``, ``"double"``, ``"string"``, or a DataType-like object).

Internally, ``pandas_udf``:

- Registers the function on the active ``SparkSession`` UDF registry as a
  ``GroupedVectorizedAgg`` UDF.
- Returns a ``UserDefinedFunction``-like object that can be called with columns.

### Usage with groupBy().agg

Use the returned grouped UDF in ``groupBy().agg(...)``:

```python
df = spark.createDataFrame(
    [(1, 10.0), (1, 20.0), (2, 5.0), (2, 15.0)],
    ["k", "v"],
)

grouped = df.group_by(["k"])
result = grouped.agg([mean_udf(rs.col("v")).alias("mean_v")])
rows = sorted(result.collect(), key=lambda r: r["k"])
assert rows == [
    {"k": 1, "mean_v": 15.0},
    {"k": 2, "mean_v": 10.0},
]
```

### Semantics and constraints

- **Input**:
  - Each argument is passed as a Python sequence (list) of values for **one group**.
  - Groups are defined by the preceding ``group_by([...])`` keys.
- **Output**:
  - The UDF **must return exactly one scalar value per group** (or ``None``).
  - Returning a sequence is not supported and will raise a clear error.
- **Supported contexts**:
  - ``DataFrame.group_by([...]).agg([pandas_udf(..., function_type="grouped_agg")(...).alias(...), ...])``
- **Unsupported contexts**:
  - Using grouped vectorized UDFs outside ``groupBy().agg`` (e.g. in ``with_column``, ``select``,
    filters, joins, or SQL) is not supported in v1. Attempting to use a grouped-agg UDF in these
    contexts raises a clear runtime error indicating that grouped UDFs are only supported in
    ``groupBy().agg(...)``.
- **Mixing with built-in aggregations**:
  - In v1, a single ``groupBy().agg(...)`` call must contain **either**:
    - only built-in aggregations (``sum``, ``avg``, etc.), or
    - only grouped vectorized UDFs.
  - Mixing grouped UDFs and built-ins in the same ``agg`` call raises ``NotImplementedError``.
    You can run them in separate ``agg`` calls and join results if needed.

### Performance

- Grouped vectorized UDFs are evaluated **per group in Python**, not lazily in Polars.
- They are substantially slower than built-in aggregations and should only be used when a
  built-in equivalent does not exist.
- Internally, robin-sparkless:
  - Uses Polars groupBy + ``implode()`` to materialize per-group lists of argument values.
  - Calls the Python UDF once per group (per aggregation), then stitches the results back.

As with all Python UDFs, prefer native expressions/aggregations whenever possible.

### Configuration (Python UDF batch size and concurrency)

When using the Python bindings (PyO3), you can control how non-grouped **vectorized** UDFs are batched
by setting Spark-style config keys on the `SparkSession` builder:

```python
spark = (
    rs.SparkSession.builder()
    .app_name("udf-config")
    .config("spark.robin.pythonUdf.batchSize", "1024")
    .config("spark.robin.pythonUdf.maxConcurrentBatches", "4")
    .get_or_create()
)
```

- **`spark.robin.pythonUdf.batchSize`** (string-encoded integer):
  - Controls how many rows are passed to a column-wise vectorized UDF per Python call.
  - Larger batches reduce call overhead but increase memory at the UDF boundary.
- **`spark.robin.pythonUdf.maxConcurrentBatches`**:
  - Reserved for future concurrency controls. At present, Python’s GIL means UDF execution is still
    effectively single-threaded per process; this knob is parsed and stored but not used to run UDFs
    truly in parallel.

These settings do **not** affect grouped vectorized UDFs (`function_type="grouped_agg"`), which are
always invoked once per group.

## Rust UDFs

### Registration

```rust
use robin_sparkless::session::SparkSession;
use polars::prelude::{DataType, Series};

let spark = SparkSession::builder().app_name("test").get_or_create();
spark.register_udf("to_str", |cols| cols[0].cast(&DataType::String))?;
```

### Usage

```rust
use robin_sparkless::functions::{call_udf, col};

let col = call_udf("to_str", &[col("id")])?;
let df2 = df.with_column("id_str", &col)?;
```

Rust UDFs run **lazily** via Polars `Expr::map` / `map_many`; no materialization at the UDF boundary.

## SQL

Register a UDF, then use it in SQL:

```sql
SELECT id, double(id) AS doubled, name FROM t
SELECT id, to_str(id) AS id_str FROM t
```

Built-in functions (e.g. `UPPER`, `LOWER`) are also supported. Unknown function names resolve to the UDF registry.

## Plan interpreter

UDFs can be used in `execute_plan` with `withColumn`:

```json
{"op": "withColumn", "payload": {"name": "id_str", "expr": {"udf": "to_str", "args": [{"col": "id"}]}}}
```

Or:

```json
{"op": "withColumn", "payload": {"name": "id_str", "expr": {"fn": "call_udf", "args": [{"lit": "to_str"}, {"col": "id"}]}}}
```

## Session scope

UDFs are **session-scoped**. Register on the session; they are visible to that session’s DataFrame operations, SQL, and plan execution. Use `SparkSession.builder().get_or_create()` so the thread-local session is set for `call_udf` resolution.

## Deferred

- **pandas_udf decorator**: Full PySpark-style `pandas_udf` decorator for **all** function types
  (scalar, grouped map, map, etc.) remains partially deferred. In v1 only a minimal
  ``pandas_udf(..., function_type="grouped_agg")`` is implemented for grouped aggregations in
  `groupBy().agg`. For scalar/vectorized (non-grouped) UDFs, continue to use
  `spark.udf().register(name, f, return_type=..., vectorized=True)` instead.
- **UDTF**: Table functions returning multiple rows are not supported.
