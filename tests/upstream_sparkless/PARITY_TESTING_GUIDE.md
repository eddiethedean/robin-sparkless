# PySpark Parity Testing Guide

This guide explains the unified PySpark parity testing approach used in Sparkless. All tests validate that Sparkless behaves identically to PySpark using pre-generated expected outputs.

## Overview

All tests in Sparkless are structured as **PySpark parity tests**. This means:

1. **Every test compares Sparkless results against PySpark's expected behavior**
2. **Expected outputs are pre-generated from PySpark** and stored as JSON files
3. **Tests run without PySpark as a runtime dependency** (fast local testing)
4. **Tests are organized by feature area**, not by test type

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              PYSPARK PARITY TESTING SYSTEM                  │
└─────────────────────────────────────────────────────────────┘

┌─────────────────┐         ┌──────────────────┐
│   PySpark       │         │   Expected       │
│   Generator     │────────>│   Outputs        │
│   (Offline)     │         │   (JSON)         │
└─────────────────┘         └──────────────────┘
                                     │
                                     │ Load
                                     ▼
┌─────────────────┐         ┌──────────────────┐
│   Sparkless     │         │   Comparison     │
│   Runtime       │────────>│   Engine         │
│   (Tests)       │         │                  │
└─────────────────┘         └──────────────────┘
                                     │
                                     ▼
                            ┌──────────────────┐
                            │   Pass/Fail      │
                            │   Report         │
                            └──────────────────┘
```

## Directory Structure

```
tests/
├── fixtures/
│   └── parity_base.py          # Base class for all parity tests
├── tools/
│   ├── generate_expected_outputs.py  # PySpark output generator
│   ├── output_loader.py              # Expected output loader
│   └── comparison_utils.py           # Comparison utilities
├── expected_outputs/           # Pre-generated PySpark outputs
│   ├── dataframe/              # DataFrame operations
│   ├── functions/              # SQL functions
│   ├── aggregations/           # Aggregation operations
│   ├── joins/                  # Join operations
│   ├── window/                 # Window functions
│   └── sql/                    # SQL execution
└── parity/                     # All PySpark parity tests
    ├── dataframe/              # DataFrame operation parity
    ├── functions/              # Function parity
    ├── sql/                    # SQL execution parity
    └── types/                  # Type system parity
```

## Test Pattern

All parity tests follow this standard pattern:

```python
"""
PySpark parity tests for [FEATURE].

Tests validate that Sparkless behaves identically to PySpark.
"""

import pytest
from tests.fixtures.parity_base import ParityTestBase
from sparkless import F


class Test[Feature]Parity(ParityTestBase):
    """Test [feature] parity with PySpark."""

    def test_[operation](self, spark):
        """Test [operation] matches PySpark behavior."""
        expected = self.load_expected("category", "test_name")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.[operation](...)
        
        self.assert_parity(result, expected)
```

## Writing New Tests

### Step 1: Use ParityTestBase

All tests inherit from `ParityTestBase` which provides:

- `spark` fixture: Creates and cleans up Sparkless sessions
- `load_expected()`: Loads expected output from JSON
- `assert_parity()`: Compares Sparkless result with expected output

### Step 2: Load Expected Output

```python
expected = self.load_expected("category", "test_name")
```

Categories include: `dataframe`, `functions`, `aggregations`, `joins`, `sql`, etc.

### Step 3: Execute Sparkless Operation

Use the input data from expected output:

```python
df = spark.createDataFrame(expected["input_data"])
result = df.select(F.upper(df.name))
```

### Step 4: Assert Parity

```python
self.assert_parity(result, expected)
```

This compares:
- Schema (field names, types, nullable flags)
- Row count
- Data values (with tolerance for floating point)

## Adding New Test Cases

### Step 1: Generate Expected Output

Add test case to `tests/tools/generate_expected_outputs.py`:

```python
def generate_dataframe_operations(self):
    test_cases = [
        ("new_test_name", lambda: df.new_operation()),
    ]
    for test_name, operation in test_cases:
        result_df = operation()
        self._save_expected_output("category", test_name, test_data, result_df)
```

### Step 2: Run Generator

```bash
python tests/tools/generate_expected_outputs.py --category category_name
```

### Step 3: Create Test

```python
def test_new_operation(self, spark):
    """Test new operation matches PySpark behavior."""
    expected = self.load_expected("category", "new_test_name")
    df = spark.createDataFrame(expected["input_data"])
    result = df.new_operation()
    self.assert_parity(result, expected)
```

## Running Tests

### Run All Parity Tests

```bash
pytest tests/parity/
```

### Run Specific Category

```bash
pytest tests/parity/dataframe/
pytest tests/parity/functions/
```

### Run Specific Test

```bash
pytest tests/parity/dataframe/test_select.py::TestSelectParity::test_basic_select
```

## Expected Output Format

Each expected output file is a JSON file with this structure:

```json
{
  "test_id": "test_name",
  "pyspark_version": "3.5",
  "generated_at": "2024-01-15T10:30:00Z",
  "input_data": [
    {"id": 1, "name": "Alice", "value": 100.5}
  ],
  "expected_output": {
    "schema": {
      "field_count": 1,
      "field_names": ["result"],
      "field_types": ["double"],
      "fields": [
        {"name": "result", "type": "double", "nullable": true}
      ]
    },
    "data": [
      {"result": 100.5}
    ],
    "row_count": 1
  }
}
```

## Comparison Details

The `assert_parity()` method compares:

1. **Row Count**: Must match exactly
2. **Schema**:
   - Field count
   - Field names
   - Field types
   - Nullable flags
3. **Data**:
   - Values with tolerance for floating point (default: 1e-6)
   - Null handling
   - Type compatibility

## Troubleshooting

### Missing Expected Output

**Error**: `FileNotFoundError: Expected output file not found`

**Solution**: Generate the missing expected output:
```bash
python tests/tools/generate_expected_outputs.py --category category_name
```

### Schema Mismatch

**Error**: `Schema field names mismatch`

**Solution**: 
- Check Sparkless function/operation generates same column names as PySpark
- Update Sparkless implementation if needed

### Data Mismatch

**Error**: `Value mismatch` or `Numerical mismatch`

**Solution**:
- Verify Sparkless calculation logic matches PySpark
- Check for precision issues (use appropriate tolerance)
- Verify null handling matches PySpark

### Row Count Mismatch

**Error**: `Row count mismatch`

**Solution**:
- Check filtering logic matches PySpark
- Verify distinct/unique operations match
- Check join behavior matches PySpark

## Best Practices

1. **Use ParityTestBase**: Always inherit from `ParityTestBase` for consistency
2. **Descriptive Test Names**: Use clear names that describe what's being tested
3. **One Operation Per Test**: Keep tests focused on single operations
4. **Use Expected Input Data**: Always use `expected["input_data"]` for creating DataFrames
5. **Document Edge Cases**: Add comments for non-obvious test scenarios
6. **Test Null Handling**: Include tests with null values where applicable

## Migration from Old Test Structure

Old test structures (`tests/unit/`, `tests/compatibility/`, `tests/api_parity/`) are being migrated to the unified `tests/parity/` structure.

### Migration Checklist

- [ ] Move test to appropriate `tests/parity/` subdirectory
- [ ] Update to inherit from `ParityTestBase`
- [ ] Use `self.load_expected()` instead of direct `load_expected_output()`
- [ ] Use `self.assert_parity()` instead of `assert_dataframes_equal()`
- [ ] Ensure expected output exists in `tests/expected_outputs/`
- [ ] Update imports to use `tests.fixtures.parity_base`
- [ ] Remove old test file after verification

## CI/CD Integration

### Fast Local Testing

Tests run with Sparkless only (no PySpark dependency):
```bash
pytest tests/parity/  # Fast - uses pre-generated outputs
```

### Full Validation

To regenerate expected outputs (requires PySpark):
```bash
python tests/tools/generate_expected_outputs.py --all
```

### Continuous Integration

CI should:
1. Run all parity tests (fast, no PySpark needed)
2. Periodically regenerate expected outputs (e.g., weekly or on PySpark version changes)
3. Validate that all tests pass

## Success Criteria

A successful parity test:

1. ✅ Loads expected output from JSON
2. ✅ Executes Sparkless operation
3. ✅ Compares result with expected output
4. ✅ All comparisons pass (schema, row count, data)
5. ✅ Test runs quickly (no PySpark startup overhead)

## Questions?

- See existing test files in `tests/parity/` for examples
- Check `tests/fixtures/parity_base.py` for base class documentation
- Review `tests/tools/comparison_utils.py` for comparison logic details

