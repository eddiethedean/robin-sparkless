# Quick Start Guide - Compatibility Testing

## TL;DR

```bash
# Run all compatibility tests
./run_compatibility_tests.sh

# Check results
cat COMPATIBILITY_REPORT.md
```

## What It Does

Tests sparkless against **20 combinations** of Python (3.9-3.13) and PySpark (3.2-3.5) versions using Docker.

## Prerequisites

✅ Docker installed and running  
✅ 5-10 GB disk space  
✅ 30-60 minutes time  

## Quick Commands

### Run Full Matrix
```bash
./run_compatibility_tests.sh
```

### Test Single Combination
```bash
./tests/compatibility_matrix/test_single_combination.sh 3.10 3.3.4
```

### Run Programmatically
```bash
python tests/compatibility_matrix/run_matrix_tests.py
```

## What Gets Tested

Each combination runs:
- ✓ Package import
- ✓ Basic DataFrame operations  
- ✓ Data types
- ✓ SQL operations
- ✓ PySpark compatibility

## Output

`COMPATIBILITY_REPORT.md` with:
- Summary (passed/failed counts)
- Matrix table (visual compatibility grid)
- Detailed results per combination
- Error messages for failures

## Troubleshooting

**Docker not running?**
```bash
docker info  # Check status
```

**Out of disk space?**
```bash
docker system prune -a  # Clean up
```

**Want to test faster?**
Edit `run_matrix_tests.py` to test fewer combinations.

## Example Output

```
Sparkless Compatibility Matrix

**Total combinations tested:** 20
**Passed:** 18 ✓
**Failed:** 2 ✗

| Python | PySpark 3.2 | PySpark 3.3 | PySpark 3.4 | PySpark 3.5 |
|--------|-------------|-------------|-------------|-------------|
| 3.9    | ✓ Pass      | ✓ Pass      | ✓ Pass      | ✓ Pass      |
| 3.10   | ✓ Pass      | ✓ Pass      | ✓ Pass      | ✗ Fail      |
...
```

## Need More Details?

See [README.md](README.md) for comprehensive documentation.

