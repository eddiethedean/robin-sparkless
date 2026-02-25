# Compatibility Matrix Testing

This directory contains tools for testing sparkless compatibility across multiple Python and PySpark version combinations using Docker.

## Overview

The compatibility testing system:
- Tests sparkless against Python 3.9-3.13
- Tests against PySpark 3.2, 3.3, 3.4, and 3.5
- Uses Docker to ensure clean, isolated test environments
- Generates a comprehensive compatibility report

## Prerequisites

- Docker installed and running
- Sufficient disk space for multiple Docker images (~2-3 GB per combination)
- Internet connection for downloading Docker images

## Quick Start

Run all compatibility tests:

```bash
cd /path/to/sparkless
python tests/compatibility_matrix/run_matrix_tests.py
```

This will:
1. Build Docker images for each Python/PySpark combination
2. Run a subset of critical tests in each container
3. Generate `COMPATIBILITY_REPORT.md` with results

## Test Coverage

The compatibility tests run a representative subset of tests:
- Package import verification
- Basic DataFrame operations
- Data type handling
- SQL operations
- PySpark compatibility

This provides reasonable confidence in compatibility without running the full test suite (which would take significantly longer).

## Files

- `Dockerfile.template` - Parameterized Dockerfile for building test images
- `test_runner.sh` - Script that runs tests inside containers
- `run_matrix_tests.py` - Orchestrates all test combinations
- `README.md` - This file

## Output

The test run generates `COMPATIBILITY_REPORT.md` in the project root with:
- Summary statistics
- Compatibility matrix table
- Detailed results for each combination
- Error messages for failed combinations

## Customization

To modify which versions are tested, edit the `get_python_versions()` and `get_pyspark_versions()` methods in `run_matrix_tests.py`.

## Troubleshooting

### Docker not running
Ensure Docker Desktop is running before starting tests.

### Out of disk space
Remove old Docker images:
```bash
docker system prune -a
```

### Test timeout
Some combinations may take longer. Adjust the timeout in `run_matrix_tests.py` (default: 300 seconds).

### Specific combination fails
Check the detailed error in `COMPATIBILITY_REPORT.md`. Some combinations may be inherently incompatible.

