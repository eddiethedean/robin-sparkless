## Category: Window functions (~25+ failures)

**Errors:** assert 100 == 90, duplicate output name in window, expected leading integer in the duration string found 'm' (date_trunc)

Subcategories: window orderBy list, date_trunc duration format, row_number/rank over descending, window plus arithmetic/withField.

### Reproduction (sparkless, fails)

```python
from sparkless.sql import SparkSession
from sparkless.sql.functions import row_number
from sparkless.sql.window import Window

spark = SparkSession.builder.app_name("test").get_or_create()
df = spark.createDataFrame([(1, 90), (2, 100)], ["id", "v"])
w = Window.orderBy("v")  # orderBy as list may differ
# date_trunc("month", col("d")): duration string format (1 month vs m)
# row_number().over(Window.orderBy(col("v").desc()))
```

### Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, date_trunc
from pyspark.sql import Window

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, 90), (2, 100)], ["id", "v"])
df.withColumn("rn", row_number().over(Window.orderBy("v"))).collect()
df.withColumn("m", date_trunc("month", col("ts"))).collect()
```

### Tests to pass or fix

- tests/upstream_sparkless/tests/parity/functions/test_window_orderby_list_parity.py
- tests/upstream_sparkless/tests/test_issue_335_window_orderby_list.py
- tests/upstream_sparkless/tests/unit/functions/test_date_trunc_robust.py
- tests/upstream_sparkless/tests/unit/functions/test_date_trunc_polars_backend.py
- tests/upstream_sparkless/tests/test_issue_414_row_number_over_descending.py
- tests/upstream_sparkless/tests/test_issue_398_withfield_window.py
- tests/upstream_sparkless/tests/unit/dataframe/test_window_arithmetic.py
- tests/upstream_sparkless/tests/parity/dataframe/test_window.py

**Action:** Window orderBy as list; date_trunc duration parsing; window frame/ordering and duplicate names.
