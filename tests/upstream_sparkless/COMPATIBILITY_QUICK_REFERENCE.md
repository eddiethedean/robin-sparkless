# Compatibility Testing Quick Reference

## Quick Commands

```bash
# Generate all expected outputs
bash tests/generate_expected_outputs.sh

# Generate specific category
python tests/tools/generate_expected_outputs.py --category functions

# Run compatibility tests
pytest tests/compatibility/

# Run specific test file
pytest tests/compatibility/test_functions_compatibility.py -v
```

## Test File Template

```python
"""
Compatibility tests for [CATEGORY] using expected outputs.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal
from sparkless import F


class Test[Category]Compatibility:
    """Test [category] compatibility against expected PySpark outputs."""
    
    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        from sparkless import MockSparkSession
        session = MockSparkSession("[category]_test")
        yield session
        session.stop()
    
    def test_[function_name](self, spark):
        """Test [function] against expected outputs."""
        expected = load_expected_output("[category]", "[test_name]")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.[function](df.[column]))
        
        assert_dataframes_equal(result, expected)
```

## Generator Template

```python
def _generate_[category]_outputs(self):
    """Generate expected outputs for [category]."""
    test_cases = {
        "[test_name]": lambda df: df.select(F.[function](df.[column])),
        "[test_name2]": lambda df: df.groupBy("[col]").agg(F.[agg_function](df.[value])),
    }
    
    for test_name, test_func in test_cases.items():
        self._generate_single_output("[category]", test_name, test_func)
```

## Common Test Patterns

### String Functions
```python
# Basic string function
result = df.select(F.upper(df.name))

# String function with parameters
result = df.select(F.substring(df.name, 1, 3))

# String function with multiple columns
result = df.select(F.concat(df.name, F.lit(" - "), df.email))
```

### Math Functions
```python
# Basic math function
result = df.select(F.sqrt(df.value))

# Math function with parameters
result = df.select(F.round(df.value, 2))

# Math function with multiple columns
result = df.select(F.greatest(df.a, df.b, df.c))
```

### Aggregations
```python
# Single aggregation
result = df.groupBy("category").agg(F.sum(df.value))

# Multiple aggregations
result = df.groupBy("category").agg(
    F.sum(df.value).alias("total"),
    F.avg(df.value).alias("average"),
    F.count("*").alias("count")
)
```

### Joins
```python
# Inner join
result = df1.join(df2, df1.id == df2.id, "inner")

# Left join
result = df1.join(df2, df1.id == df2.id, "left")

# Join with multiple conditions
result = df1.join(df2, (df1.id == df2.id) & (df1.status == df2.status))
```

### Conditional Functions
```python
# When/otherwise
result = df.select(F.when(df.age > 30, "Senior").otherwise("Junior"))

# Coalesce
result = df.select(F.coalesce(df.col1, df.col2, df.col3))

# Case statements
result = df.select(
    F.when(df.score >= 90, "A")
    .when(df.score >= 80, "B")
    .when(df.score >= 70, "C")
    .otherwise("F")
)
```

## Expected Output Structure

```json
{
  "test_id": "unique_identifier",
  "pyspark_version": "3.2",
  "generated_at": "2024-01-15T10:30:00Z",
  "input_data": [
    {"id": 1, "name": "Alice", "value": 100.5}
  ],
  "expected_output": {
    "schema": {
      "field_count": 1,
      "field_names": ["function_name(column)"],
      "field_types": ["double"],
      "fields": [
        {
          "name": "function_name(column)",
          "type": "double",
          "nullable": true
        }
      ]
    },
    "data": [
      {"function_name(column)": 150.75}
    ],
    "row_count": 1
  }
}
```

## Troubleshooting

| Error | Solution |
|-------|----------|
| `FileNotFoundError: Expected output file not found` | Run generator: `python tests/tools/generate_expected_outputs.py --category [category]` |
| `Schema field names mismatch` | Check Sparkless function naming matches PySpark |
| `Numerical mismatch` | Verify Sparkless calculation logic matches PySpark |
| `Row count mismatch` | Check Sparkless operation produces same number of rows |
| `Null mismatch` | Verify null handling in Sparkless matches PySpark |

## File Locations

- **Test files**: `tests/compatibility/test_*_compatibility.py`
- **Generator**: `tests/tools/generate_expected_outputs.py`
- **Expected outputs**: `tests/expected_outputs/[category]/[test_name].json`
- **Output loader**: `tests/tools/output_loader.py`
- **Comparison utils**: `tests/tools/comparison_utils.py`
