## Category: Missing native API – `arrays_overlap`

**Error:** `AttributeError: module 'sparkless._native' has no attribute 'arrays_overlap'`

Array-contains-join and related tests fail because the Rust extension does not expose `arrays_overlap`. PySpark uses this for joins on array overlap (e.g. `array_contains`-style join conditions).

### Reproduction (sparkless – fails)

```python
from sparkless.sql import SparkSession
from sparkless.sql.functions import array_contains, col

spark = SparkSession.builder.app_name("test").get_or_create()
# Any code path that triggers arrays_overlap (e.g. join using array overlap)
# fails with: AttributeError: module 'sparkless._native' has no attribute 'arrays_overlap'
import sparkless._native as n
_ = n.arrays_overlap  # AttributeError
```

### Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import arrays_overlap, col

spark = SparkSession.builder.getOrCreate()
left = spark.createDataFrame([({"id": 1, "arr": [1, 2]}), ({"id": 2, "arr": [3, 4]})], ["id", "arr"])
right = spark.createDataFrame([({"id": 1, "brr": [2, 5]}), ({"id": 3, "brr": [1, 2]})], ["id", "brr"])
result = left.join(right, arrays_overlap(col("arr"), col("brr"))).collect()
# Returns rows where arrays have at least one common element.
```

### Tests to pass or fix

- `tests/upstream_sparkless/tests/parity/functions/test_array_contains_join_parity.py` (all)
- `tests/upstream_sparkless/tests/test_issue_331_array_contains_join.py` (all, ~30 tests)

**Action:** Implement and expose `arrays_overlap` in the Rust extension and Python bindings (or equivalent join semantics).
