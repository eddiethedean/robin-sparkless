## Category: Pow / coalesce / head / SparkContext / union(DataFrame-like) (~15+ failures)

**Errors:** unsupported operand type(s) for pow(): PyColumn and int, AttributeError collect, list has no attribute collect, str object is not callable, Wrapper cannot be converted to PyDataFrame

Subcategories: pow / power operator, coalesce return type, head() API, SparkContext.version, union(DataFrame-like).

### Reproduction (sparkless, fails)

```python
from sparkless.sql import SparkSession
from sparkless.sql.functions import col, pow

spark = SparkSession.builder.app_name("test").get_or_create()
df = spark.createDataFrame([(3,), (5,)], ["x"])
# Column power: col("x") ** 2 or pow(col("x"), 2) -> TypeError
df.select(pow(col("x"), 2).alias("sq")).collect()
df.head()   # AttributeError: list has no attribute collect (or wrong return type)
df.head(2)
spark.sparkContext.version  # TypeError if version is method not property
# coalesce: result type int vs string
# left.union(duck_typed_df)  # other cannot be converted to PyDataFrame
```

### Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(3,), (5,)], ["x"])
df.select((col("x").cast("int") * col("x")).alias("sq")).collect()  # 9, 25
# or pow(col("x"), 2)
df.head()   # single Row or list of Row
df.head(2)  # list of 2 Row
spark.sparkContext.version  # string e.g. "3.5.0"
df.select(coalesce(col("a"), col("b"), lit(0))).collect()
```

### Tests to pass or fix

- tests/python/test_issue_405_pow_bitwise.py (all)
- tests/python/test_issue_407_coalesce_variadic.py (all)
- tests/python/test_issue_413_head.py (all)
- tests/python/test_issue_387_spark_context.py (all)
- tests/upstream_sparkless/tests/test_sparkcontext_validation.py
- tests/python/test_issue_385_union_dataframe_like.py (all)

**Action:** Implement Column power (pow); fix coalesce result types; implement head() and SparkContext.version; consider DataFrame-like union or document limitation.
