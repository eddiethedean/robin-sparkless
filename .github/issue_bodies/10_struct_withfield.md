## Category: Struct / withField / getField (~25+ failures)

**Errors:** field not found E1/k/Value, assert None == 1, Expected struct for nested access StructValue

Subcategories: struct field alias, withField, getField/subscript.

### Reproduction (sparkless, fails)

```python
from sparkless.sql import SparkSession
from sparkless.sql.functions import col

spark = SparkSession.builder.app_name("test").get_or_create()
# Struct field selection with alias
# df.select(col("struct_col").getField("name").alias("n")) -> field not found or wrong result
# withField: update struct field
# Nested access on non-struct -> clear error
```

### Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([({"name": "Alice", "age": 25},)], ["s"])
df.select(col("s").getField("name").alias("n")).first().n  # "Alice"
df.withColumn("s2", col("s").withField("age", col("s.age") + 1))
```

### Tests to pass or fix

- tests/upstream_sparkless/tests/test_issue_330_struct_field_alias.py
- tests/upstream_sparkless/tests/parity/functions/test_struct_field_alias_parity.py
- tests/upstream_sparkless/tests/unit/dataframe/test_withfield.py
- tests/upstream_sparkless/tests/test_issue_398_withfield_window.py
- tests/upstream_sparkless/tests/test_issue_358_getfield.py
- tests/upstream_sparkless/tests/test_issue_339_column_subscript.py

**Action:** Align struct field resolution, alias propagation, withField/getField with PySpark.
