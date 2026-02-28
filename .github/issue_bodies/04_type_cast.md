## Category: Type / cast API and semantics (~20+ failures)

**Errors:** `AttributeError: 'IntegerType' object has no attribute 'simpleString'`, `SparklessError: conversion from str to i32/i64 failed`, cast result (null vs error for invalid string)

Subcategories: DataType.simpleString, string→int cast semantics, cast/alias select.

### Reproduction (sparkless – fails)

```python
from sparkless.sql import SparkSession
from sparkless.sql.functions import col
from sparkless.sql.types import IntegerType, LongType, StringType

# simpleString() missing on some DataType subclasses
t = IntegerType()
t.simpleString()  # AttributeError

# String to int: PySpark returns NULL for invalid strings; sparkless may raise
spark = SparkSession.builder.app_name("test").get_or_create()
df = spark.createDataFrame([("hello",), ("123",), (None,)], ["s"])
df.select(col("s").cast("int")).collect()  # PySpark: Row(s=None), Row(s=123), Row(s=None)
# sparkless: may raise conversion failed for "hello"
```

### Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

t = IntegerType()
assert t.simpleString() == "int"

df = spark.createDataFrame([("hello",), ("123",), (None,)], ["s"])
df.select(col("s").cast("int")).collect()
# [Row(s=None), Row(s=123), Row(s=None)]  -- invalid string -> NULL
```

### Tests to pass or fix

- `tests/python/test_issue_394_cast_data_type.py` (all)
- `tests/python/test_issue_217_string_to_int_cast.py` (all)
- `tests/upstream_sparkless/tests/parity/functions/test_cast_alias_select_parity.py` (all)

**Action:** Add/fix `simpleString()` on all DataType subclasses; implement string→int semantics (invalid → NULL) to match PySpark.
