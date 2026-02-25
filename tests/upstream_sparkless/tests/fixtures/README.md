# Test Fixtures and Utilities

This directory contains the unified test infrastructure for running tests with both PySpark and sparkless.

## Overview

The unified test infrastructure allows you to:
- Run the same tests with either PySpark or sparkless
- Compare results between backends
- Use PySpark as a baseline for sparkless behavior validation

## Key Components

### `spark_backend.py`
Backend abstraction layer that creates SparkSession instances from either PySpark or sparkless based on configuration.

**Key Functions:**
- `get_backend_type()` - Get backend from markers/env/default
- `SparkBackend.create_session()` - Create session for specified backend
- `SparkBackend.create_sessions_for_comparison()` - Create both sessions

### `spark_imports.py`
Unified import abstraction that automatically selects the correct imports based on backend configuration.

**Usage:**
```python
from tests.fixtures.spark_imports import get_spark_imports

SparkSession, F, StructType = get_spark_imports()
```

### `comparison.py`
Utilities for comparing results between PySpark and sparkless.

**Key Functions:**
- `assert_dataframes_equal()` - Assert two DataFrames are equal
- `compare_dataframes()` - Compare DataFrames and return result
- `compare_schemas()` - Compare schema structures

### `result_capture.py`
Capture and store test results for baseline generation and comparison.

### `difference_handlers.py`
Handlers for known differences between PySpark and sparkless (row ordering, precision, etc.).

## Usage

See `tests/TESTING_WITH_PYSPARK.md` and `tests/MIGRATION_TO_UNIFIED_TESTS.md` for detailed usage instructions.

## Examples

See `tests/examples/test_unified_infrastructure_example.py` for example tests demonstrating the infrastructure.

