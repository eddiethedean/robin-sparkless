# Quick Start Guide - Compatibility Testing

## TL;DR

```bash
# Run all compatibility tests
./run_compatibility_tests.sh

# Check results
cat COMPATIBILITY_REPORT.md
```

## What It Does

Tests sparkless against known-compatible combinations of Python 3.11-3.13 and PySpark 3.4-3.5 using Docker. Python 3.8-3.10 are no longer supported by this release.

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
./tests/compatibility_matrix/test_single_combination.sh 3.11 3.5.1 17
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

**Total combinations tested:** 5
**Passed:** 5 ✓
**Failed:** 0 ✗

| Python | PySpark 3.4 | PySpark 3.5 |
|--------|-------------|-------------|
| 3.11   | ✓ Pass      | ✓ Pass      |
| 3.12   | ✓ Pass      | ✓ Pass      |
| 3.13   | —           | ✓ Pass      |
```

## Need More Details?

See [README.md](README.md) for comprehensive documentation.

