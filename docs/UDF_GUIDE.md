# UDF Guide

Robin-sparkless supports **scalar user-defined functions** (UDFs) in PySpark style. Both **Python UDFs** and **Rust UDFs** are supported.

## Python UDFs

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
df = spark.create_dataframe([(1, 10, "a"), (2, 20, "b")], ["id", "v", "name"])
df2 = df.with_column("doubled", my_udf(rs.col("id")))
```

Or call by name:

```python
df2 = df.with_column("d2", rs.call_udf("double", rs.col("id")))
```

### Return types

`return_type` can be omitted (defaults to `StringType`), a DDL string (`"int"`, `"bigint"`, `"string"`, `"double"`, `"boolean"`, `"date"`, `"timestamp"`), or a DataType-like object with `typeName`.

### Limitations

- **Row-at-a-time execution**: Python UDFs run row-by-row; data is materialized at the UDF boundary.
- **WHERE/HAVING**: Python UDFs in SQL WHERE or HAVING are not yet supported.
- **Plan interpreter**: Python UDFs work in `withColumn`; not in filter expressions.

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

- **pandas_udf**: Vectorized UDFs (Pandas Series in/out) are deferred.
- **UDTF**: Table functions returning multiple rows are not supported.
