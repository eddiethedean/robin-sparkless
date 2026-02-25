# Migration Guide: Unified Test Infrastructure

This guide explains how to migrate existing tests to use the unified test infrastructure that supports both PySpark and sparkless.

## Overview

The unified test infrastructure allows tests to run with either sparkless or PySpark (or both for comparison) without code changes. This enables:
- Direct comparison testing
- PySpark as baseline validation
- Easy switching between backends

## Migration Steps

### Step 1: Update Imports (Optional)

For new tests or when refactoring, use unified imports:

**Before:**
```python
from sparkless import SparkSession
from sparkless.sql import functions as F
from sparkless.sql.types import StructType, StructField, StringType
```

**After (Optional - for new tests):**
```python
from tests.fixtures.spark_imports import get_spark_imports

SparkSession, F, StructType = get_spark_imports()
# Or use the full imports object
from tests.fixtures.spark_imports import SparkImports
imports = SparkImports()
SparkSession = imports.SparkSession
F = imports.F
```

**Note**: Existing imports continue to work. This is only needed if you want tests to automatically switch backends.

### Step 2: Use Unified `spark` Fixture

The `spark` fixture automatically selects the backend based on configuration:

**Before:**
```python
@pytest.fixture
def spark():
    from sparkless import SparkSession
    session = SparkSession("test_app")
    yield session
    session.stop()
```

**After:**
```python
# No changes needed! The unified spark fixture handles this
def test_something(spark):
    df = spark.createDataFrame([{"id": 1}])
    assert df.count() == 1
```

### Step 3: Add Backend Markers (Optional)

Add markers to control which backend runs the test:

```python
# Run only with PySpark
@pytest.mark.backend('pyspark')
def test_pyspark_specific(spark):
    # This test only runs with PySpark
    pass

# Run only with sparkless
@pytest.mark.backend('mock')
def test_mock_specific(spark):
    # This test only runs with sparkless
    pass

# Run with both and compare
@pytest.mark.backend('both')
def test_comparison(sparkless_session, pyspark_session):
    # Both sessions available for comparison
    from tests.fixtures.comparison import assert_dataframes_equal
    
    mock_df = sparkless_session.createDataFrame([{"id": 1}])
    pyspark_df = pyspark_session.createDataFrame([{"id": 1}])
    
    assert_dataframes_equal(mock_df, pyspark_df)
```

### Step 4: Update Comparison Tests

If you have existing comparison tests, update them to use the new utilities:

**Before:**
```python
def test_comparison(sparkless, pyspark_spark):
    mock_df = sparkless.createDataFrame([{"id": 1}])
    pyspark_df = pyspark_spark.createDataFrame([{"id": 1}])
    
    # Manual comparison
    assert mock_df.count() == pyspark_df.count()
    assert mock_df.collect() == pyspark_df.collect()
```

**After:**
```python
@pytest.mark.backend('both')
def test_comparison(sparkless_session, pyspark_session):
    from tests.fixtures.comparison import assert_dataframes_equal
    
    mock_df = sparkless_session.createDataFrame([{"id": 1}])
    pyspark_df = pyspark_session.createDataFrame([{"id": 1}])
    
    # Automatic comparison with normalization
    assert_dataframes_equal(mock_df, pyspark_df)
```

## Common Patterns

### Pattern 1: Backend-Agnostic Tests

Most tests don't need changes - they work with both backends automatically:

```python
def test_basic_operations(spark):
    """This test works with both sparkless and PySpark."""
    df = spark.createDataFrame([{"id": 1, "name": "Alice"}])
    result = df.filter(df.id > 0).select("name").collect()
    assert len(result) == 1
```

### Pattern 2: Backend-Specific Tests

Use markers to specify which backend to use:

```python
@pytest.mark.backend('pyspark')
def test_pyspark_feature(spark):
    """Test PySpark-specific behavior."""
    # This only runs with PySpark
    pass

@pytest.mark.backend('mock')
def test_mock_feature(spark):
    """Test sparkless-specific behavior."""
    # This only runs with sparkless
    pass
```

### Pattern 3: Comparison Tests

Use comparison mode to validate sparkless against PySpark:

```python
@pytest.mark.backend('both')
def test_validation(sparkless_session, pyspark_session):
    """Validate sparkless matches PySpark behavior."""
    from tests.fixtures.comparison import assert_dataframes_equal
    
    data = [{"id": i, "value": i * 2} for i in range(10)]
    
    mock_df = sparkless_session.createDataFrame(data)
    pyspark_df = pyspark_session.createDataFrame(data)
    
    # Compare basic operations
    assert_dataframes_equal(mock_df, pyspark_df)
    
    # Compare filtered results
    mock_filtered = mock_df.filter(mock_df.id > 5)
    pyspark_filtered = pyspark_df.filter(pyspark_df.id > 5)
    assert_dataframes_equal(mock_filtered, pyspark_filtered)
```

### Pattern 4: Handling Differences

When differences are expected, use difference handlers:

```python
from tests.fixtures.difference_handlers import normalize_row_ordering

@pytest.mark.backend('both')
def test_with_differences(sparkless_session, pyspark_session):
    """Test that handles known differences."""
    # ... create DataFrames ...
    
    mock_rows = mock_df.collect()
    pyspark_rows = pyspark_df.collect()
    
    # Normalize row ordering (PySpark doesn't guarantee order)
    mock_rows = normalize_row_ordering(mock_rows, mock_df.columns)
    pyspark_rows = normalize_row_ordering(pyspark_rows, pyspark_df.columns)
    
    # Now compare
    assert mock_rows == pyspark_rows
```

## Migration Checklist

- [ ] Review test file for backend-specific code
- [ ] Update imports if using unified import abstraction (optional)
- [ ] Ensure `spark` fixture is used (already works with unified fixture)
- [ ] Add backend markers if test should run with specific backend
- [ ] Update comparison tests to use comparison utilities
- [ ] Test with both backends: `MOCK_SPARK_TEST_BACKEND=both pytest tests/your_test.py`
- [ ] Verify test passes with both backends

## Backward Compatibility

**Important**: All existing tests continue to work without changes. The unified infrastructure is backward compatible:

- Existing `spark` fixture still works (now uses unified backend)
- Existing imports still work
- Existing test patterns still work

Migration is optional and can be done gradually.

## Examples

### Example 1: Simple Test (No Changes Needed)

```python
# This test works with both backends automatically
def test_count(spark):
    df = spark.createDataFrame([{"id": 1}, {"id": 2}])
    assert df.count() == 2
```

### Example 2: Comparison Test

```python
@pytest.mark.backend('both')
def test_aggregation_comparison(sparkless_session, pyspark_session):
    from tests.fixtures.comparison import assert_dataframes_equal
    
    data = [{"category": "A", "value": 10}, {"category": "B", "value": 20}]
    
    mock_df = sparkless_session.createDataFrame(data)
    pyspark_df = pyspark_session.createDataFrame(data)
    
    mock_result = mock_df.groupBy("category").agg({"value": "sum"})
    pyspark_result = pyspark_df.groupBy("category").agg({"value": "sum"})
    
    assert_dataframes_equal(mock_result, pyspark_result, tolerance=1e-6)
```

### Example 3: Backend-Specific Test

```python
@pytest.mark.backend('pyspark')
def test_pyspark_behavior(spark):
    """Test that requires PySpark-specific behavior."""
    # This test only runs with PySpark
    df = spark.createDataFrame([{"id": 1}])
    # ... test PySpark-specific features ...
```

## Troubleshooting

### Test Fails with PySpark but Passes with Sparkless

This indicates a difference in behavior. Options:
1. Fix sparkless to match PySpark behavior
2. Document the difference as expected
3. Use difference handlers to normalize results

### Test Fails with Both Backends

This indicates a test issue, not a backend difference. Fix the test logic.

### Import Errors

If you see import errors with unified imports:
- Ensure `tests/fixtures/` is in Python path
- Check that you're using the correct import syntax
- Fall back to direct imports if needed

## Next Steps

1. Start with simple tests to get familiar with the infrastructure
2. Migrate comparison tests to use new utilities
3. Add backend markers to tests that need specific backends
4. Run tests with both backends to validate behavior

