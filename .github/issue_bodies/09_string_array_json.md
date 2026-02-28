## Category: String / array / JSON functions (~50+ failures)

**Errors:** DataFrames not equivalent, xxhash64 value mismatch, list.eval not supported for dtype str

Subcategories: levenshtein/xxhash64/get_json_object/json_tuple, substring_index/regexp_extract, split limit, array contains/explode/posexplode alias and lengths, array/map type in arrays.

### Reproduction (sparkless, fails)

```python
from sparkless.sql import SparkSession
from sparkless.sql.functions import levenshtein, xxhash64, get_json_object, split

spark = SparkSession.builder.app_name("test").get_or_create()
df = spark.createDataFrame([("kitten", "sitting")], ["a", "b"])
df.select(levenshtein("a", "b").alias("d")).collect()  # PySpark: 3
# xxhash64: hash value may differ from PySpark
# get_json_object / json_tuple: result or null handling
# split(col, pattern, limit): limit parameter
# explode/posexplode: alias or result length
```

### Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import levenshtein, xxhash64, get_json_object, split, col

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([("kitten", "sitting")], ["a", "b"])
df.select(levenshtein("a", "b").alias("d")).first().d  # 3
df.select(xxhash64("a")).first()
df.select(split(col("s"), ",", 2)).collect()
```

### Tests to pass or fix

- tests/upstream_sparkless/tests/parity/functions/test_string.py
- tests/upstream_sparkless/tests/unit/functions/test_issue_189_string_functions_robust.py
- tests/upstream_sparkless/tests/test_issue_328_split_limit.py
- tests/upstream_sparkless/tests/test_issue_293_explode_withcolumn.py
- tests/upstream_sparkless/tests/test_issue_366_alias_posexplode.py
- tests/upstream_sparkless/tests/test_issue_429_posexplode_no_alias.py
- tests/upstream_sparkless/tests/test_issue_430_posexplode_alias_execution.py
- tests/upstream_sparkless/tests/unit/spark_types/test_array_type_robust.py
- tests/upstream_sparkless/tests/test_issue_339_column_subscript.py
- tests/upstream_sparkless/tests/test_issue_441_map_column_subscript.py
- tests/upstream_sparkless/tests/parity/functions/test_array.py

**Action:** Align string/array/JSON results and nulls; split limit; explode/posexplode alias and length; map/struct in arrays.
