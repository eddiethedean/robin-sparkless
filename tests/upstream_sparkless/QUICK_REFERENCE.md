# Quick Reference: Unified Test Infrastructure

## Running Tests

### Default (Sparkless)
```bash
pytest tests/
# or
MOCK_SPARK_TEST_BACKEND=mock pytest tests/
```

### With PySpark
```bash
MOCK_SPARK_TEST_BACKEND=pyspark pytest tests/
```

### Comparison Mode (Both Backends)
```bash
MOCK_SPARK_TEST_BACKEND=both pytest tests/
# or use the comparison runner
bash tests/run_comparison_tests.sh
```

## Test Markers

```python
# Run only with sparkless
@pytest.mark.backend('mock')
def test_mock_only(spark):
    pass

# Run only with PySpark
@pytest.mark.backend('pyspark')
def test_pyspark_only(spark):
    pass

# Run with both and compare
@pytest.mark.backend('both')
def test_comparison(sparkless_session, pyspark_session):
    from tests.fixtures.comparison import assert_dataframes_equal
    # Compare results
```

## Fixtures

### `spark`
Unified fixture that works with both backends:
```python
def test_something(spark):
    df = spark.createDataFrame([{"id": 1}])
```

### `sparkless_session`
Explicit sparkless session:
```python
def test_mock(spark):
    # Uses sparkless
    pass
```

### `pyspark_session`
Explicit PySpark session (skips if unavailable):
```python
def test_pyspark(pyspark_session):
    # Uses PySpark
    pass
```

### `spark_backend`
Get current backend type:
```python
def test_backend_info(spark, spark_backend):
    backend_name = spark_backend.value  # 'mock' or 'pyspark'
```

## Comparison Utilities

```python
from tests.fixtures.comparison import assert_dataframes_equal, compare_dataframes

# Simple assertion
assert_dataframes_equal(mock_df, pyspark_df)

# With options
assert_dataframes_equal(
    mock_df, pyspark_df,
    tolerance=1e-6,
    check_schema=True,
    check_order=False
)

# Get comparison result
is_equal, error_msg = compare_dataframes(mock_df, pyspark_df)
```

## Unified Imports

```python
from tests.fixtures.spark_imports import get_spark_imports

SparkSession, F, StructType = get_spark_imports()
# Automatically selects based on backend
```

## Examples

See `tests/examples/test_unified_infrastructure_example.py` for complete examples.

