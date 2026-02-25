# Compatibility Testing Guide

This guide explains how to add new compatibility tests using the new expected-output architecture. This system allows us to test Sparkless against PySpark without requiring PySpark as a runtime dependency.

## Architecture Overview

The compatibility testing system has three main components:

1. **Expected Output Generator** (`tests/tools/generate_expected_outputs.py`) - Runs PySpark code to generate "golden" outputs
2. **Output Loader** (`tests/tools/output_loader.py`) - Loads and caches expected outputs during tests
3. **Comparison Utils** (`tests/tools/comparison_utils.py`) - Compares Sparkless results with expected outputs

### Key Benefits

- **No PySpark Runtime Dependency**: Tests run without PySpark installed
- **Deterministic Results**: Expected outputs are pre-generated and versioned
- **Fast Test Execution**: No need to start PySpark sessions during testing
- **Version Control**: Expected outputs are stored in JSON files and can be versioned
- **Easy Debugging**: Clear comparison of actual vs expected results

## Directory Structure

```
tests/
├── tools/                          # Test infrastructure
│   ├── generate_expected_outputs.py    # PySpark output generator
│   ├── output_loader.py               # Expected output loader
│   └── comparison_utils.py            # Comparison utilities
├── expected_outputs/               # Generated expected outputs
│   ├── functions/                  # Function tests
│   ├── dataframe_operations/       # DataFrame operation tests
│   ├── joins/                      # Join operation tests
│   ├── aggregations/               # Aggregation tests
│   └── ...                        # Other categories
├── compatibility/                  # Compatibility test files
│   ├── test_functions_compatibility.py
│   ├── test_joins_compatibility.py
│   └── ...
└── generate_expected_outputs.sh   # Generation script
```

## How to Add New Compatibility Tests

### Step 1: Add Test Case to Generator

First, add your test case to the `ExpectedOutputGenerator` class in `tests/tools/generate_expected_outputs.py`:

```python
def _generate_your_category_outputs(self):
    """Generate expected outputs for your category."""
    test_cases = {
        "your_test_name": lambda df: df.select(F.your_function(df.column)),
        "another_test": lambda df: df.groupBy("col").agg(F.sum(df.value)),
    }
    
    for test_name, test_func in test_cases.items():
        self._generate_single_output("your_category", test_name, test_func)
```

### Step 2: Create Test Data

Define input data for your tests. The generator will use this data with both PySpark and Sparkless:

```python
# In the generator, define input data
input_data = [
    {"id": 1, "name": "Alice", "value": 100.5},
    {"id": 2, "name": "Bob", "value": 200.0},
    {"id": 3, "name": "Charlie", "value": None},
]
```

### Step 3: Generate Expected Outputs

Run the generation script to create expected output files:

```bash
# Generate all outputs
bash tests/generate_expected_outputs.sh

# Generate specific category
python tests/tools/generate_expected_outputs.py --category your_category

# Generate for specific PySpark version
python tests/tools/generate_expected_outputs.py --category your_category --pyspark-version 3.5
```

### Step 4: Create Compatibility Test File

Create a new test file in `tests/compatibility/`:

```python
"""
Compatibility tests for your category using expected outputs.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal
from sparkless import F


class TestYourCategoryCompatibility:
    """Test your category compatibility against expected PySpark outputs."""
    
    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        from sparkless import MockSparkSession
        session = MockSparkSession("your_category_test")
        yield session
        session.stop()
    
    def test_your_test_name(self, spark):
        """Test your function against expected outputs."""
        expected = load_expected_output("your_category", "your_test_name")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.your_function(df.column))
        
        assert_dataframes_equal(result, expected)
    
    def test_another_test(self, spark):
        """Test another function against expected outputs."""
        expected = load_expected_output("your_category", "another_test")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("col").agg(F.sum(df.value))
        
        assert_dataframes_equal(result, expected)
```

### Step 5: Run Tests

Run your new compatibility tests:

```bash
# Run specific test file
pytest tests/compatibility/test_your_category_compatibility.py

# Run all compatibility tests
pytest tests/compatibility/

# Run with verbose output
pytest tests/compatibility/test_your_category_compatibility.py -v
```

## Expected Output File Format

Each expected output file is a JSON file with this structure:

```json
{
  "test_id": "unique_test_identifier",
  "pyspark_version": "3.2",
  "generated_at": "2024-01-15T10:30:00Z",
  "input_data": [
    {"id": 1, "name": "Alice", "value": 100.5},
    {"id": 2, "name": "Bob", "value": 200.0}
  ],
  "expected_output": {
    "schema": {
      "field_count": 1,
      "field_names": ["your_function(column)"],
      "field_types": ["double"],
      "fields": [
        {
          "name": "your_function(column)",
          "type": "double",
          "nullable": true
        }
      ]
    },
    "data": [
      {"your_function(column)": 150.75},
      {"your_function(column)": 200.0}
    ],
    "row_count": 2
  }
}
```

## Best Practices

### 1. Test Data Design

- **Use realistic data**: Include edge cases like nulls, empty strings, extreme values
- **Keep data small**: Tests should be fast and focused
- **Include variety**: Test different data types and scenarios

```python
# Good test data
input_data = [
    {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0},
    {"id": 2, "name": "Bob", "age": None, "salary": 60000.0},
    {"id": 3, "name": "", "age": 30, "salary": None},
    {"id": 4, "name": "Charlie", "age": 35, "salary": 70000.0},
]
```

### 2. Test Naming

- **Use descriptive names**: `test_string_upper_with_mixed_case` not `test_upper`
- **Group related tests**: Use consistent prefixes for related functionality
- **Include edge cases**: `test_math_sqrt_with_negative_numbers`

### 3. Error Handling

- **Test error conditions**: Invalid inputs, type mismatches
- **Use appropriate assertions**: Check for specific error types
- **Document expected behavior**: Add docstrings explaining test purpose

### 4. Performance Considerations

- **Keep tests focused**: One test per specific behavior
- **Use fixtures efficiently**: Reuse common setup
- **Avoid complex operations**: Keep test data and operations simple

## Common Patterns

### 1. String Function Tests

```python
def test_string_upper(self, spark):
    """Test upper function against expected outputs."""
    expected = load_expected_output("functions", "string_upper")
    
    df = spark.createDataFrame(expected["input_data"])
    result = df.select(F.upper(df.name))
    
    assert_dataframes_equal(result, expected)
```

### 2. Math Function Tests

```python
def test_math_sqrt(self, spark):
    """Test sqrt function against expected outputs."""
    expected = load_expected_output("functions", "math_sqrt")
    
    df = spark.createDataFrame(expected["input_data"])
    result = df.select(F.sqrt(df.value))
    
    assert_dataframes_equal(result, expected)
```

### 3. Aggregation Tests

```python
def test_agg_sum(self, spark):
    """Test sum aggregation against expected outputs."""
    expected = load_expected_output("functions", "agg_sum")
    
    df = spark.createDataFrame(expected["input_data"])
    result = df.groupBy("category").agg(F.sum(df.value))
    
    assert_dataframes_equal(result, expected)
```

### 4. Join Tests

```python
def test_inner_join(self, spark):
    """Test inner join against expected outputs."""
    expected = load_expected_output("joins", "inner_join")
    
    df1 = spark.createDataFrame(expected["input_data"]["left"])
    df2 = spark.createDataFrame(expected["input_data"]["right"])
    result = df1.join(df2, df1.id == df2.id, "inner")
    
    assert_dataframes_equal(result, expected)
```

## Troubleshooting

### 1. Missing Expected Output Files

**Error**: `FileNotFoundError: Expected output file not found`

**Solution**: Generate the missing expected outputs:
```bash
python tests/tools/generate_expected_outputs.py --category your_category
```

### 2. Schema Mismatches

**Error**: `Schema field names mismatch: mock=['col1'], expected=['col2']`

**Solution**: Check that Sparkless and PySpark generate the same column names. You may need to update Sparkless's function implementations.

### 3. Data Mismatches

**Error**: `Numerical mismatch in column 'value' row 0: mock=1.0, expected=2.0`

**Solution**: Verify that Sparkless's implementation produces the same results as PySpark. Check for:
- Different calculation logic
- Type conversion issues
- Precision differences

### 4. Generation Failures

**Error**: PySpark generation fails with specific functions

**Solution**: 
- Check PySpark version compatibility
- Use equivalent functions for different PySpark versions
- Add error handling in the generator

## Advanced Usage

### 1. Custom Comparison Logic

For complex data types, you can extend the comparison utilities:

```python
def custom_compare_values(mock_val, expected_val, tolerance, context):
    """Custom comparison for specific data types."""
    if isinstance(mock_val, CustomType) and isinstance(expected_val, CustomType):
        return mock_val.equals(expected_val), ""
    # Fall back to default comparison
    return _compare_values(mock_val, expected_val, tolerance, context)
```

### 2. Version-Specific Tests

Test against multiple PySpark versions:

```python
@pytest.mark.parametrize("pyspark_version", ["3.2", "3.3", "3.4"])
def test_function_with_version(self, spark, pyspark_version):
    """Test function against specific PySpark version."""
    expected = load_expected_output("functions", "test_name", pyspark_version)
    # ... test implementation
```

### 3. Conditional Test Generation

Generate different test cases based on PySpark version:

```python
def _generate_version_specific_tests(self):
    """Generate tests specific to PySpark version."""
    if self.pyspark_version >= "3.3":
        # New features available in 3.3+
        test_cases["new_feature"] = lambda df: df.select(F.new_feature(df.col))
    else:
        # Fallback for older versions
        test_cases["legacy_feature"] = lambda df: df.select(F.legacy_feature(df.col))
```

## Maintenance

### 1. Updating Expected Outputs

When PySpark behavior changes or new versions are released:

```bash
# Regenerate all outputs
bash tests/generate_expected_outputs.sh

# Regenerate specific category
python tests/tools/generate_expected_outputs.py --category functions --pyspark-version 3.5
```

### 2. Adding New PySpark Versions

1. Update the `PYSPARK_VERSIONS` array in `generate_expected_outputs.sh`
2. Add version-specific test cases in the generator
3. Regenerate expected outputs for the new version

### 3. Cleaning Up

Remove outdated expected output files:

```bash
# Remove old version files
find tests/expected_outputs -name "*_3_0.json" -delete
find tests/expected_outputs -name "*_3_1.json" -delete
```

## Conclusion

This architecture provides a robust, maintainable way to test Sparkless compatibility with PySpark. The separation of concerns between generation, loading, and comparison makes it easy to add new tests and debug issues.

For questions or issues, refer to the existing test files as examples or consult the development team.