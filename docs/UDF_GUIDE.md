# User-defined functions

Applies to the installed **Sparkless 4.18.0 local Python API**. The HTTP and
Spark Connect services do not execute Python UDFs. Prefer built-in expressions
for performance; scalar Python UDFs collect input and run Python once per row.

## Typed scalar Python UDF

```python
from sparkless.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.app_name("udf-example").get_or_create()
try:
    double = F.udf(lambda x: None if x is None else x * 2, T.IntegerType())
    df = spark.createDataFrame([(2,), (None,)], [("x", "int")])
    result = df.withColumn("doubled", double(F.col("x")))
    assert [row["doubled"] for row in result.collect()] == [4, None]
    assert df.filter(double(F.col("x")) >= 4).count() == 1
finally:
    spark.stop()
```

`F.udf(f=None, returnType=None)` returns a callable that builds a UDF column.
Use an explicit `sparkless.sql.types.DataType` object for `returnType`.
Omitting it defaults to string output. Handle `None` in your function; nulls are
passed to Python. The function must return values compatible with its declared
type. Python exceptions propagate during evaluation; do not use UDF side effects
for correctness. Materialization can exhaust memory on large inputs.

The tested contract covers `withColumn`, projection, multiple arguments, and
scalar comparison filters (`==`, `!=`, `<`, `<=`, `>`, `>=`). This does not imply
arbitrary composed predicates, UDF join keys, grouped aggregates, or SQL UDF
registration work. The public `sparkless.sql.functions` module does not expose
`pandas_udf`; pandas/vectorized/grouped Python UDFs and UDTFs are unsupported.
Historical binding experiments are not the installed Python API.

Run the [executable UDF example](https://github.com/eddiethedean/robin-sparkless/blob/main/examples/python/test_udf_example.py)
on both engines as described in [Testing](TESTING_GUIDE.md).

## Rust extensions

Rust consumers can register native expression UDFs with `SparkSession::register_udf`.
These are engine callbacks and have a different contract from Python `F.udf`.
See the [Rust reference fallback](RUST_API_REFERENCE.md),
[logical plan format](LOGICAL_PLAN_FORMAT.md), and [extension author guide](EXTENDING.md).
