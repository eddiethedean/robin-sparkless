# PySpark Parity Tests

This directory contains all PySpark parity tests for Sparkless. Every test validates that Sparkless behaves identically to PySpark using pre-generated expected outputs.

## Structure

```
parity/
├── dataframe/          # DataFrame operation parity tests
├── functions/          # SQL function parity tests
├── sql/               # SQL execution parity tests
└── internal/          # Internal features with PySpark equivalents
```

## Running Tests

### Run All Parity Tests

```bash
# From project root
pytest tests/parity/

# Or use the dedicated script
./tests/run_parity_tests.sh
```

### Run Specific Category

```bash
pytest tests/parity/dataframe/
pytest tests/parity/functions/
pytest tests/parity/sql/
```

### Run Specific Test

```bash
pytest tests/parity/dataframe/test_select.py::TestSelectParity::test_basic_select
```

## Test Pattern

All tests inherit from `ParityTestBase` and follow this pattern:

```python
from tests.fixtures.parity_base import ParityTestBase

class TestFeatureParity(ParityTestBase):
    def test_operation(self, spark):
        expected = self.load_expected("category", "test_name")
        df = spark.createDataFrame(expected["input_data"])
        result = df.operation(...)
        self.assert_parity(result, expected)
```

## Expected Outputs

Expected outputs are pre-generated from PySpark and stored in `tests/expected_outputs/`.
They are organized by category matching the test structure.

## Adding New Tests

See `tests/PARITY_TESTING_GUIDE.md` for detailed instructions on adding new parity tests.

## Known Issues

Some tests may be blocked by bugs. See `tests/BUG_LOG.md` for details.

