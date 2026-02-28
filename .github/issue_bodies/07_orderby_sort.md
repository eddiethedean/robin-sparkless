## Category: Order by / sort (~15+ failures)

**Errors:** assert [1,2,3] == [3,2,1], `TypeError: orderBy() expects column names as str or Column/SortOrder expressions`

Subcategories: ascending=False not applied, orderBy(list).

### Reproduction (sparkless, fails)

```python
from sparkless.sql import SparkSession
from sparkless.sql.functions import col

spark = SparkSession.builder.app_name("test").get_or_create()
df = spark.createDataFrame([{"a": 3}, {"a": 1}, {"a": 2}], [("a", "int")])
out = df.orderBy(col("a"), ascending=False).collect()
# Expected: [3, 2, 1]
# Actual: [1, 2, 3] (ascending ignored)
```

### Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(3,), (1,), (2,)], ["a"])
df.orderBy(col("a").desc()).collect()   # [Row(a=3), Row(a=2), Row(a=1)]
df.orderBy("a", ascending=False).collect()
df.orderBy([col("a"), col("b")], ascending=False).collect()
```

### Tests to pass or fix

- tests/python/test_issue_378_order_by_ascending_bool.py (all)
- tests/upstream_sparkless/tests/test_issue_327_orderby_ascending.py
- tests/upstream_sparkless/tests/test_issue_335_window_orderby_list.py
- tests/upstream_sparkless/tests/test_issue_415_orderby_list.py
- tests/upstream_sparkless/tests/parity/functions/test_window_orderby_list_parity.py

**Action:** Ensure orderBy accepts Column/SortOrder and ascending=False; support list of columns.
