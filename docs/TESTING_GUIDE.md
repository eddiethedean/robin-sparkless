# sparkless.testing Guide

The `sparkless.testing` module provides a unified framework for writing tests that run against both **sparkless** (Rust/Polars backend) and **PySpark** (JVM backend). This enables you to:

- Write tests once, run against both backends
- Validate your code produces identical results on both engines
- Run fast local tests with sparkless, and integration tests with PySpark
- Use consistent fixtures and comparison utilities

---

## Quick Start

### 1. Add the pytest plugin to your `conftest.py`

```python
# conftest.py
pytest_plugins = ["sparkless.testing"]
```

This automatically registers fixtures (`spark`, `spark_mode`, `spark_imports`, etc.) and pytest markers.

### 2. Write a test using the `spark` fixture

```python
def test_filter(spark):
    df = spark.createDataFrame([
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ])
    result = df.filter(df.id > 1).collect()
    assert len(result) == 1
    assert result[0]["name"] == "Bob"
```

### 3. Run tests

```bash
# Run with sparkless (default, fast)
pytest tests/

# Run with PySpark (validates parity)
SPARKLESS_TEST_MODE=pyspark pytest tests/
```

---

## Environment Variable

The test backend is controlled by the `SPARKLESS_TEST_MODE` environment variable:

| Value | Backend | Use Case |
|-------|---------|----------|
| `sparkless` (default) | Sparkless (Rust/Polars) | Fast local tests, CI |
| `pyspark` | PySpark (JVM) | Parity validation, integration tests |

```bash
# Fast local tests
pytest tests/

# Validate against PySpark
SPARKLESS_TEST_MODE=pyspark pytest tests/

# Explicit sparkless mode
SPARKLESS_TEST_MODE=sparkless pytest tests/
```

---

## Fixtures

### `spark`

The main fixture providing a `SparkSession` for the current mode.

```python
def test_create_dataframe(spark):
    df = spark.createDataFrame([{"x": 1}, {"x": 2}])
    assert df.count() == 2
```

### `spark_mode`

Returns the current `Mode` enum (`Mode.SPARKLESS` or `Mode.PYSPARK`).

```python
from sparkless.testing import Mode

def test_mode_specific_behavior(spark, spark_mode):
    df = spark.createDataFrame([{"id": 1}])
    
    if spark_mode == Mode.PYSPARK:
        # PySpark-specific assertion
        assert hasattr(df, "_jdf")
    else:
        # Sparkless-specific assertion
        pass
```

### `spark_imports`

Provides mode-appropriate imports (SparkSession, functions, types).

```python
def test_with_imports(spark, spark_imports):
    F = spark_imports.F
    df = spark.createDataFrame([{"name": "alice"}])
    result = df.select(F.upper("name")).collect()
    assert result[0][0] == "ALICE"
```

### `isolated_session`

Creates a fresh, isolated SparkSession (useful for tests that modify session state).

```python
def test_isolated(isolated_session):
    spark = isolated_session
    spark.conf.set("my.custom.config", "value")
    # This session is independent of other tests
```

### `table_prefix`

Provides a unique prefix for table names (useful when sharing sessions).

```python
def test_with_table(spark, table_prefix):
    df = spark.createDataFrame([{"id": 1}])
    table_name = f"{table_prefix}_my_table"
    df.write.saveAsTable(table_name)
    # Table name is unique per test
```

---

## Markers

### `@pytest.mark.sparkless_only`

Skip test when running in PySpark mode.

```python
@pytest.mark.sparkless_only
def test_sparkless_specific_feature(spark):
    # This test only runs in sparkless mode
    pass
```

### `@pytest.mark.pyspark_only`

Skip test when running in sparkless mode.

```python
@pytest.mark.pyspark_only
def test_pyspark_specific_feature(spark):
    # This test only runs in PySpark mode
    pass
```

### `@pytest.mark.backend("sparkless")` / `@pytest.mark.backend("pyspark")`

Force a specific backend for a test (overrides environment variable).

```python
@pytest.mark.backend("pyspark")
def test_always_pyspark(spark):
    # This test always uses PySpark
    pass
```

---

## Direct API Usage

You can also use `sparkless.testing` directly without pytest fixtures.

### Mode Detection

```python
from sparkless.testing import Mode, get_mode, is_pyspark_mode, is_sparkless_mode

mode = get_mode()  # Mode.SPARKLESS or Mode.PYSPARK

if is_pyspark_mode():
    print("Running with PySpark")
elif is_sparkless_mode():
    print("Running with sparkless")
```

### Session Creation

```python
from sparkless.testing import create_session, Mode

# Create session for current mode
spark = create_session(app_name="my_test")

# Create session for specific mode
sparkless_spark = create_session(app_name="test", mode=Mode.SPARKLESS)
pyspark_spark = create_session(app_name="test", mode=Mode.PYSPARK)
```

### Unified Imports

```python
from sparkless.testing import get_imports

imports = get_imports()

# Access Spark classes and functions
SparkSession = imports.SparkSession
F = imports.F  # functions module
Window = imports.Window
Row = imports.Row

# Data types
StructType = imports.StructType
StructField = imports.StructField
StringType = imports.StringType
IntegerType = imports.IntegerType
# ... and more
```

---

## DataFrame Comparison

The module provides utilities for comparing DataFrames, which is essential for parity testing.

### `assert_dataframes_equal`

Assert two DataFrames are equivalent.

```python
from sparkless.testing import assert_dataframes_equal

def test_transform(spark):
    input_df = spark.createDataFrame([{"x": 1}, {"x": 2}])
    
    result = input_df.select(input_df.x * 2)
    expected = spark.createDataFrame([{"(x * 2)": 2}, {"(x * 2)": 4}])
    
    assert_dataframes_equal(result, expected)
```

### Options

```python
assert_dataframes_equal(
    actual_df,
    expected_df,
    tolerance=1e-6,       # Float comparison tolerance
    check_schema=True,    # Compare schemas
    check_order=False,    # Ignore row order
)
```

### `compare_dataframes`

Get detailed comparison results without raising an exception.

```python
from sparkless.testing import compare_dataframes

result = compare_dataframes(df1, df2)

if result.equivalent:
    print("DataFrames match!")
else:
    print("Differences found:")
    for error in result.errors:
        print(f"  - {error}")
```

### `assert_rows_equal`

Compare row collections directly.

```python
from sparkless.testing import assert_rows_equal

rows1 = df1.collect()
rows2 = df2.collect()

assert_rows_equal(rows1, rows2, check_order=False)
```

---

## Complete Example: Dual-Mode Test Suite

Here's a complete example of a test file using `sparkless.testing`:

```python
"""Tests for my_transform module."""

import pytest
from sparkless.testing import (
    Mode,
    get_imports,
    assert_dataframes_equal,
)


class TestMyTransform:
    """Test suite for data transformations."""

    def test_basic_filter(self, spark):
        """Test basic filtering works on both backends."""
        df = spark.createDataFrame([
            {"id": 1, "status": "active"},
            {"id": 2, "status": "inactive"},
            {"id": 3, "status": "active"},
        ])
        
        result = df.filter(df.status == "active")
        
        assert result.count() == 2

    def test_aggregation(self, spark, spark_imports):
        """Test aggregation with functions."""
        F = spark_imports.F
        
        df = spark.createDataFrame([
            {"dept": "IT", "salary": 100},
            {"dept": "IT", "salary": 200},
            {"dept": "HR", "salary": 150},
        ])
        
        result = df.groupBy("dept").agg(
            F.sum("salary").alias("total"),
            F.avg("salary").alias("avg"),
        )
        
        rows = {r["dept"]: r for r in result.collect()}
        assert rows["IT"]["total"] == 300
        assert rows["HR"]["total"] == 150

    def test_window_function(self, spark, spark_imports):
        """Test window functions."""
        F = spark_imports.F
        Window = spark_imports.Window
        
        df = spark.createDataFrame([
            {"dept": "IT", "name": "Alice", "salary": 100},
            {"dept": "IT", "name": "Bob", "salary": 200},
            {"dept": "HR", "name": "Charlie", "salary": 150},
        ])
        
        window = Window.partitionBy("dept").orderBy(F.desc("salary"))
        result = df.withColumn("rank", F.rank().over(window))
        
        rows = {r["name"]: r for r in result.collect()}
        assert rows["Bob"]["rank"] == 1  # Highest in IT
        assert rows["Alice"]["rank"] == 2

    @pytest.mark.sparkless_only
    def test_sparkless_native_feature(self, spark, spark_imports):
        """Test sparkless-specific functionality."""
        # Access sparkless native module
        if spark_imports._native is not None:
            # Test native functionality
            pass

    def test_dataframe_comparison(self, spark):
        """Test DataFrame comparison utilities."""
        df1 = spark.createDataFrame([
            {"id": 1, "value": 10.0},
            {"id": 2, "value": 20.0},
        ])
        df2 = spark.createDataFrame([
            {"id": 2, "value": 20.0},
            {"id": 1, "value": 10.0},
        ])
        
        # Order doesn't matter
        assert_dataframes_equal(df1, df2, check_order=False)

    def test_with_schema(self, spark, spark_imports):
        """Test explicit schema definition."""
        StructType = spark_imports.StructType
        StructField = spark_imports.StructField
        StringType = spark_imports.StringType
        IntegerType = spark_imports.IntegerType
        
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ])
        
        df = spark.createDataFrame(
            [{"name": "Alice", "age": 30}],
            schema=schema,
        )
        
        assert df.schema.fields[0].name == "name"
        assert df.schema.fields[1].name == "age"
```

---

## CI Configuration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test-sparkless:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -e ./python[test]
      - run: pytest tests/ -v

  test-pyspark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - uses: actions/setup-java@v4
        with:
          distribution: "temurin"
          java-version: "11"
      - run: pip install -e ./python[test,pyspark]
      - run: SPARKLESS_TEST_MODE=pyspark pytest tests/ -v
```

---

## API Reference

### Mode Enum

```python
from sparkless.testing import Mode

Mode.SPARKLESS  # Sparkless backend
Mode.PYSPARK    # PySpark backend
```

### Functions

| Function | Description |
|----------|-------------|
| `get_mode()` | Get current test mode from environment |
| `is_pyspark_mode()` | Check if running in PySpark mode |
| `is_sparkless_mode()` | Check if running in sparkless mode |
| `set_mode(mode)` | Set the test mode programmatically |
| `create_session(app_name, mode)` | Create a SparkSession |
| `get_imports(mode)` | Get mode-appropriate imports |

### Comparison Functions

| Function | Description |
|----------|-------------|
| `compare_dataframes(actual, expected, ...)` | Compare DataFrames, return result |
| `assert_dataframes_equal(actual, expected, ...)` | Assert DataFrames are equal |
| `assert_rows_equal(actual, expected, ...)` | Assert row collections are equal |

### SparkImports Attributes

| Attribute | Description |
|-----------|-------------|
| `SparkSession` | The SparkSession class |
| `F` / `functions` | The functions module |
| `Window` | Window class for window functions |
| `Row` | Row class |
| `StructType`, `StructField` | Schema types |
| `StringType`, `IntegerType`, `LongType`, etc. | Data types |
| `_native` | Sparkless native module (None for PySpark) |

---

## Best Practices

### 1. Use fixtures instead of creating sessions manually

```python
# Good
def test_something(spark):
    df = spark.createDataFrame(...)

# Avoid (session cleanup issues)
def test_something():
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(...)
```

### 2. Use `spark_imports` for portable code

```python
# Good - works in both modes
def test_something(spark, spark_imports):
    F = spark_imports.F
    df.select(F.upper("name"))

# Avoid - mode-specific imports
def test_something(spark):
    from pyspark.sql import functions as F  # Only works in PySpark mode
```

### 3. Use comparison utilities for result validation

```python
# Good - handles float tolerance, order, etc.
assert_dataframes_equal(result, expected, tolerance=1e-6, check_order=False)

# Fragile - manual comparison
assert result.collect() == expected.collect()
```

### 4. Mark mode-specific tests appropriately

```python
@pytest.mark.sparkless_only
def test_native_feature(spark):
    """Test that only makes sense in sparkless mode."""
    pass

@pytest.mark.pyspark_only  
def test_jvm_feature(spark):
    """Test that requires JVM features."""
    pass
```

### 5. Use `table_prefix` for table isolation

```python
def test_with_tables(spark, table_prefix):
    table_name = f"{table_prefix}_users"
    df.write.saveAsTable(table_name)
    # No conflicts with other tests
```

---

## Troubleshooting

### PySpark session creation fails

Ensure Java is installed and `JAVA_HOME` is set:

```bash
# macOS
brew install openjdk@11
export JAVA_HOME=/opt/homebrew/opt/openjdk@11

# Ubuntu
sudo apt install openjdk-11-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

### Import errors for sparkless.testing

Ensure sparkless is installed:

```bash
pip install -e ./python
```

### Tests pass in sparkless but fail in PySpark

This indicates a parity issue. Check [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md) for known divergences, or file an issue if you've found a new one.

### Slow PySpark tests

PySpark session creation is slow (~5-10s). Use shared sessions when possible:

```bash
SPARKLESS_SHARED_SESSION=1 SPARKLESS_TEST_MODE=pyspark pytest tests/
```

---

## See Also

- [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md) - Known differences from PySpark
- [PARITY_STATUS.md](PARITY_STATUS.md) - PySpark parity coverage
- [Python README](../python/README.md) - Sparkless Python package
