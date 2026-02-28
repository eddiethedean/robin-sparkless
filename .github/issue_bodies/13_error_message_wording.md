## Category: Error message wording (~4 failures)

Tests expect substring "cannot resolve" in column-not-found errors; sparkless emits "not found: column ...".

### Reproduction (sparkless)

```python
from sparkless.sql import SparkSession

spark = SparkSession.builder.app_name("test").get_or_create()
df = spark.createDataFrame([(1,)], ["col1"])
df.select("col2").collect()  # raises; message contains "not found" but not "cannot resolve"
```

### Expected (PySpark)

PySpark often uses "cannot resolve" in analysis errors for missing columns. Tests check that the error message contains "cannot resolve".

### Tests to pass or fix

- tests/upstream_sparkless/tests/test_issue_158_dropped_column_error.py (all four tests)

**Action:** Add "cannot resolve" (or equivalent) to column-not-found error text, or relax tests to accept current message.
