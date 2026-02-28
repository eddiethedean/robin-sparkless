## Category: Join semantics and API (~30+ failures)

**Errors:** `DataFrames are not equivalent`, row shape e.g. `[(1, 10, 1, 20)]` vs `[{'id': 1, 'v': 10, 'w': 20}]`, duplicate column names

Subcategories: outer/semi/anti/cross join, join on Column, join column names/aliases, left semi.

### Reproduction (sparkless – fails)

```python
from sparkless.sql import SparkSession
from sparkless.sql.functions import col

spark = SparkSession.builder.app_name("test").get_or_create()
left = spark.createDataFrame([{"id": 1, "v": 10}], [("id", "int"), ("v", "int")])
right = spark.createDataFrame([{"id": 1, "w": 20}], [("id", "int"), ("w", "int")])

# Join on Column: PySpark returns row as dict-like with keys id, v, w
result = left.join(right, col("id")).collect()
# Expected: [Row(id=1, v=10, w=20)] or [{"id": 1, "v": 10, "w": 20}]
# Actual: [(1, 10, 1, 20)] (tuple, no keys)

# Outer/semi/anti/cross: result rows and schema may differ from PySpark
```

### Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()
left = spark.createDataFrame([(1, 10)], ["id", "v"])
right = spark.createDataFrame([(1, 20)], ["id", "w"])
result = left.join(right, col("id")).collect()
# [Row(id=1, v=10, w=20)]; result[0]["w"] == 20
left.join(right, left.id == right.id, "left_outer").collect()
left.join(right, "id", "left_semi").collect()
```

### Tests to pass or fix

- `tests/python/test_issue_353_join_on_accept_column.py` (all)
- `tests/upstream_sparkless/tests/parity/dataframe/test_join.py::TestJoinParity::test_outer_join`
- `tests/upstream_sparkless/tests/parity/dataframe/test_join.py::TestJoinParity::test_semi_join`
- `tests/upstream_sparkless/tests/parity/dataframe/test_join.py::TestJoinParity::test_anti_join`
- `tests/upstream_sparkless/tests/parity/dataframe/test_join.py::TestJoinParity::test_cross_join`
- `tests/upstream_sparkless/tests/test_issue_374_join_aliased_columns.py`
- `tests/upstream_sparkless/tests/test_issue_421_join_column_names.py`
- `tests/upstream_sparkless/tests/test_issue_438_leftsemi_join.py`
- `tests/upstream_sparkless/tests/test_issue_152_sql_column_aliases.py` (join + aliases)

**Action:** Align join types and join-on-Column result shape/names with PySpark; fix column naming in join plans.
