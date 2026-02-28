## Category: Aggregates / groupby (~25+ failures)

**Errors:** KeyError avg(Value) or approx_count_distinct(value), duplicate column name count, assert max(salary) or max_salary in row

Subcategories: aggregate column naming/alias, approx_count_distinct, duplicate column names, sum/mean on string column.

### Reproduction (sparkless, fails)

```python
from sparkless.sql import SparkSession
from sparkless.sql.functions import max as max_

spark = SparkSession.builder.app_name("test").get_or_create()
df = spark.createDataFrame([("A", 100), ("B", 150)], ["dept", "salary"])
agg = df.groupBy("dept").agg(max_("salary"))
row = agg.first()  # Expect row to have key max(salary) or max_salary
# approx_count_distinct: KeyError or missing
# Multiple aggs without alias: duplicate column name count
```

### Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, approx_count_distinct

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([("A", 100), ("B", 150)], ["dept", "salary"])
agg = df.groupBy("dept").agg(max("salary").alias("max_salary"))
agg.first()  # Row(dept='B', max_salary=150)
df.agg(approx_count_distinct("id")).collect()
```

### Tests to pass or fix

- tests/upstream_sparkless/tests/unit/test_first_method.py::test_first_after_groupby_agg
- tests/upstream_sparkless/tests/test_issue_397_groupby_alias.py
- tests/upstream_sparkless/tests/unit/functions/test_approx_count_distinct_rsd.py
- tests/upstream_sparkless/tests/parity/functions/test_approx_count_distinct_rsd_parity.py
- tests/upstream_sparkless/tests/unit/test_column_case_variations.py
- tests/upstream_sparkless/tests/test_issue_286_aggregate_function_arithmetic.py
- tests/upstream_sparkless/tests/test_issue_393_sum_string_column.py
- tests/upstream_sparkless/tests/test_issue_437_mean_string_column.py
- tests/upstream_sparkless/tests/parity/functions/test_aggregate_cast_parity.py

**Action:** Align agg naming/aliasing; implement approx_count_distinct; avoid duplicate output names in groupby.
