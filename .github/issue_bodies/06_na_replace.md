## Category: NA / replace API and semantics (~20+ failures)

**Errors:** `TypeError: replace() missing 1 required positional argument: 'value'`, `PyColumn.replace() missing 1 required positional argument: 'replacement'`, `cannot compare string with numeric type`, assert '1' == 1

Subcategories: DataFrameNaFunctions.replace signature, eqNullSafe/null comparison, fill/NA doc example.

### Reproduction (sparkless, fails)

```python
from sparkless.sql import SparkSession

spark = SparkSession.builder.app_name("test").get_or_create()
df = spark.createDataFrame([{"x": "a"}, {"x": "b"}], [("x", "string")])
# PySpark: df.na.replace("a", "A", subset=["x"])
df.na().replace("a", "A", subset=["x"])  # TypeError: replace() missing 1 required positional argument: 'value'
# Column.replace: F.col("x").replace("a", "A") - missing 'replacement'
```

### Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([("a",), ("b",)], ["x"])
df.na.replace("a", "A", subset=["x"]).collect()  # [Row(x='A'), Row(x='b')]
from pyspark.sql.functions import col
df.select(col("x").replace("a", "A")).collect()
# eqNullSafe: null eq null -> true; null eq 1 -> false
```

### Tests to pass or fix

- tests/python/test_issue_360_na_replace.py (all)
- tests/python/test_issue_379_column_replace_dict_list.py (all)
- tests/python/test_doc_examples.py::test_user_guide_na_fill_drop
- tests/upstream_sparkless/tests/test_issue_287_na_replace.py (all)
- tests/python/test_issue_248_column_eq_null_safe.py (all)
- tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py (all)

**Action:** Implement/fix DataFrameNaFunctions.replace and Column.replace signature; align eqNullSafe and NA fill types.
