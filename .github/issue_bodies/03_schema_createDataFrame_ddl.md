## Category: Schema / createDataFrame / DDL (~80+ failures)

**Errors:** `TypeError: 'list' object is not callable`, `KeyError: 'name'` / `'a'`, `AssertionError: expected TypeError`, IntegerType vs LongType, `TypeError: row 0: expected dict, list, or tuple`

Subcategories: DDL schema parsing, infer schema parity, single-type createDataFrame, pandas/schema type.

### Reproduction (sparkless – fails)

```python
from sparkless.sql import SparkSession
from sparkless.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.app_name("test").get_or_create()

# DDL string schema: may raise TypeError or KeyError
df1 = spark.createDataFrame([], "a int, b string")  # or empty data with DDL
df2 = spark.createDataFrame([(1, "x")], "id int, name string")  # KeyError: 'name' / 'a'

# Infer schema: result types/order may differ from PySpark (44 failures in test_inferschema_parity.py)
# Single-type createDataFrame: row type rejected
df3 = spark.createDataFrame([datetime.date(2024, 1, 1)], "d date")  # expected dict, list, or tuple
```

### Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df1 = spark.createDataFrame([], "a int, b string")
df2 = spark.createDataFrame([(1, "x")], "id int, name string")
df1.schema  # StructType with a (int), b (string)
df2.collect()  # [Row(id=1, name='x')]

# Infer schema: column types and order match PySpark conventions
# Single-type: accept single value per column for one row
```

### Tests to pass or fix

- `tests/python/test_issue_372_create_data_frame.py` (all)
- `tests/python/test_issue_418_nested_ddl.py` (all)
- `tests/upstream_sparkless/tests/unit/dataframe/test_inferschema_parity.py` (44 tests)
- `tests/upstream_sparkless/tests/test_issue_213_createDataFrame_with_single_type.py` (all)
- `tests/upstream_sparkless/tests/unit/test_issues_225_231.py::TestIssue229PandasDataFrameSupport` (pandas/schema type)
- `tests/upstream_sparkless/tests/test_issue_202_select_with_list.py` (select with list/tuple schema)
- `tests/upstream_sparkless/tests/test_issue_372_pandas_column_order.py`

**Action:** Align DDL parsing, createDataFrame (list/dict/Row, schema, pandas) and inferred schema with PySpark; unify IntegerType vs LongType where required.
