# Testing with PySpark

This guide explains how to run tests with PySpark to establish a baseline for sparkless behavior validation.

## Overview

The unified test infrastructure allows you to run the same tests with either sparkless or PySpark, enabling direct comparison and baseline validation. PySpark results serve as the authoritative source of truth for how sparkless should behave.

## Quick Start

### Running Tests with PySpark

```bash
# Run all tests with PySpark
MOCK_SPARK_TEST_BACKEND=pyspark pytest tests/

# Run specific test file with PySpark
MOCK_SPARK_TEST_BACKEND=pyspark pytest tests/unit/test_basic_operations.py

# Run with comparison mode (both backends)
MOCK_SPARK_TEST_BACKEND=both pytest tests/unit/test_basic_operations.py
```

### Using Pytest Markers

```python
import pytest

# Run test only with PySpark
@pytest.mark.backend('pyspark')
def test_with_pyspark(spark):
    df = spark.createDataFrame([{"id": 1}])
    assert df.count() == 1

# Run test only with sparkless
@pytest.mark.backend('mock')
def test_with_mock(spark):
    df = spark.createDataFrame([{"id": 1}])
    assert df.count() == 1

# Run test with both and compare
@pytest.mark.backend('both')
def test_comparison(sparkless_session, pyspark_session):
    from tests.fixtures.comparison import assert_dataframes_equal
    
    mock_df = sparkless_session.createDataFrame([{"id": 1}])
    pyspark_df = pyspark_session.createDataFrame([{"id": 1}])
    
    assert_dataframes_equal(mock_df, pyspark_df)
```

## Backend Selection Priority

The backend is selected in the following order (highest to lowest priority):

1. **Pytest marker**: `@pytest.mark.backend('pyspark')`
2. **Environment variable**: `MOCK_SPARK_TEST_BACKEND=pyspark`
3. **Default**: sparkless

## Comparison Mode

When using `MOCK_SPARK_TEST_BACKEND=both`, tests receive both `sparkless_session` and `pyspark_session` fixtures. Use comparison utilities to verify results match:

```python
from tests.fixtures.comparison import assert_dataframes_equal

@pytest.mark.backend('both')
def test_dataframe_operations(sparkless_session, pyspark_session):
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    
    mock_df = sparkless_session.createDataFrame(data)
    pyspark_df = pyspark_session.createDataFrame(data)
    
    # Compare results
    assert_dataframes_equal(mock_df, pyspark_df)
    
    # Test operations
    mock_result = mock_df.filter(mock_df.id > 1)
    pyspark_result = pyspark_df.filter(pyspark_df.id > 1)
    
    assert_dataframes_equal(mock_result, pyspark_result)
```

## Comparison Utilities

### Comparing DataFrames

```python
from tests.fixtures.comparison import assert_dataframes_equal, compare_dataframes

# Simple assertion
assert_dataframes_equal(mock_df, pyspark_df)

# With options
assert_dataframes_equal(
    mock_df, 
    pyspark_df,
    tolerance=1e-6,      # Floating point tolerance
    check_schema=True,  # Compare schemas
    check_order=False   # Row order doesn't matter
)

# Get comparison result
is_equal, error_msg = compare_dataframes(mock_df, pyspark_df)
if not is_equal:
    print(f"Difference found: {error_msg}")
```

### Comparing Schemas

```python
from tests.fixtures.comparison import compare_schemas

is_equal = compare_schemas(mock_df.schema, pyspark_df.schema, strict=False)
```

## Known Differences

Some differences between PySpark and sparkless are expected and handled automatically:

1. **Row Ordering**: PySpark doesn't guarantee row order - comparison utilities sort rows before comparing
2. **Floating Point Precision**: Use tolerance in comparisons
3. **Type Representations**: Some types may differ (handled by normalization)
4. **Error Messages**: May differ slightly - compare error types, not exact messages

## Handling Differences

Use difference handlers to normalize results:

```python
from tests.fixtures.difference_handlers import (
    normalize_row_ordering,
    normalize_type_representation,
)

# Normalize row ordering
normalized_rows = normalize_row_ordering(rows, columns)

# Normalize type representation
normalized_value = normalize_type_representation(value)
```

## Result Capture

Capture results for baseline generation:

```python
from tests.fixtures.result_capture import ResultCapture

capture = ResultCapture()

# Capture result
result = capture.capture_dataframe_result(
    test_name="test_basic_operations",
    mock_result=mock_df,
    pyspark_result=pyspark_df,
    metadata={"description": "Basic DataFrame operations"}
)

# Load baseline
baseline = capture.load_baseline("test_basic_operations")
```

## Best Practices

1. **Use Comparison Mode Sparingly**: Comparison mode runs tests twice, which is slower. Use it for critical tests or when validating new features.

2. **Mark Slow Tests**: PySpark tests are slower - use markers to skip them in fast CI runs:
   ```python
   @pytest.mark.backend('pyspark')
   @pytest.mark.slow
   def test_large_dataset(spark):
       # This test will be skipped in fast runs
   ```

3. **Handle PySpark Unavailability**: Always handle cases where PySpark might not be available:
   ```python
   @pytest.mark.backend('pyspark')
   def test_with_pyspark(pyspark_session):
       # pyspark_session fixture automatically skips if PySpark unavailable
       df = pyspark_session.createDataFrame([{"id": 1}])
   ```

4. **Use Unified Imports**: For new tests, use the unified import abstraction:
   ```python
   from tests.fixtures.spark_imports import get_spark_imports
   
   SparkSession, F, StructType = get_spark_imports()
   ```

## Troubleshooting

### PySpark Not Available

If PySpark is not installed, tests marked with `@pytest.mark.backend('pyspark')` will be skipped automatically. Install PySpark:

```bash
pip install pyspark
```

### Session Creation Fails

If PySpark session creation fails, check:
- Java is installed and JAVA_HOME is set
- PySpark version is compatible (3.2-3.5)
- No port conflicts (PySpark uses ports for communication)

### Comparison Failures

If comparisons fail:
1. Check if difference is expected (row ordering, precision)
2. Use appropriate tolerance for floating point comparisons
3. Normalize results using difference handlers
4. Check error messages for specific differences

## CI/CD Integration

### Fast Runs (Sparkless Only)

```bash
# Default - sparkless only, fast
pytest tests/ -m "not slow"
```

### Full Validation (PySpark Comparison)

```bash
# Run comparison tests
MOCK_SPARK_TEST_BACKEND=both pytest tests/ -m "comparison"
```

### PySpark Baseline Generation

```bash
# Generate PySpark baselines
MOCK_SPARK_TEST_BACKEND=pyspark pytest tests/ --capture-results
```

