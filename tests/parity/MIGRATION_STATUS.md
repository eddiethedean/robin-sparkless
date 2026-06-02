# Test Migration Status

This document tracks the progress of migrating all tests to the unified PySpark parity testing structure.

**Last Updated**: 2026-06-02

## Overview

Legacy directories `tests/compatibility/` and `tests/api_parity/` were removed. Active parity tests live under `tests/parity/`. Issue-numbered regression tests remain in `tests/dataframe/` and `tests/unit/`.

BUG-001 and BUG-002 (GroupedData.count / aggregate return types) are **fixed** — see `tests/BUG_LOG.md`.

## Migration Progress

### ✅ Completed Categories

#### Dataframe Operations
- ✅ **test_select.py** - Basic select, select with alias, column access (3 tests)
- ✅ **test_filter.py** - Filter operations, boolean filters (2 tests)
- ✅ **test_transformations.py** - withColumn, drop, distinct, orderBy, limit (6 tests)
- ✅ **test_join.py** - Inner, left, right, outer, cross, semi, anti joins (7 tests)
- ✅ **test_set_operations.py** - Union, unionAll, intersect, except (4 tests)
- ✅ **test_window.py** - Window functions: row_number, rank, dense_rank, sum, lag, lead, cume_dist, first_value, last_value, percent_rank, ntile (12 tests)
- ✅ **test_aggregations.py** - Sum, avg, count, max, min, multiple aggregations, nulls (9 tests)
- ✅ **test_groupby.py** - GroupBy operations

**Total**: ~45 dataframe tests migrated

#### Functions
- ✅ **test_string.py** - Core + extended string functions (trim, ltrim, rtrim, lpad, rpad, like, rlike, concat_ws, ascii, hex, base64, initcap, repeat, reverse) (21 tests)
- ✅ **test_math.py** - Core + extended math functions (sin, cos, tan, ceil, floor, greatest, least) (14 tests)
- ✅ **test_aggregate.py** - Sum, avg, count, max, min aggregations (5 tests)
- ✅ **test_array.py** - Array contains, position, size, element_at, explode, distinct, join, union, sort, remove (10 tests)
- ✅ **test_datetime.py** - Year, month, dayofmonth, dayofweek, date_add, date_sub, date_format, to_date (8 tests)
- ✅ **test_null_handling.py** - Coalesce, isnull, isnotnull, when/otherwise, nvl, nullif, ifnull, nanvl (8 tests)

**Total**: ~66 function tests migrated

#### SQL
- ✅ **test_queries.py** - Basic select, filtered select, group by (3 tests)

**Total**: 4 SQL tests migrated

### 🔄 In Progress

#### Functions (Extended)
- 🔄 Additional string functions (trim, ltrim, rtrim, lpad, rpad, like, rlike, etc.)
- 🔄 Extended math functions (sin, cos, tan, ceil, floor, greatest, least, etc.)
- 🔄 Conditional functions extended
- 🔄 Bitwise functions
- 🔄 Special functions
- 🔄 Type class functions
- 🔄 Column ordering functions
- 🔄 JSON/CSV functions
- 🔄 Map functions
- 🔄 Struct functions

#### Internal Features
- 🔄 Session operations (createDataFrame variations)
- 🔄 Catalog operations (where PySpark equivalents exist)

### ⏳ Pending

#### Test Infrastructure
- ⏳ Generate expected outputs for all new test cases
- ⏳ Verify all tests pass (some blocked by bugs)
- ⏳ Update test runners
- ⏳ Update CI/CD scripts

#### Cleanup
- ⏳ Archive or remove old test directories after verification
- ⏳ Update documentation references

## Test Count Summary

**Current Status**:
- ✅ **116 tests** migrated and collecting
- ✅ **16 test files** created in `tests/parity/`
- ✅ **271 expected outputs** available from existing compatibility tests
- ✅ **Core functionality** fully migrated and tested
- ⏳ **Extended functions** and edge cases can be migrated incrementally as needed

## Known Issues

### Blocking Bugs

1. **BUG-001**: `GroupedData.count()` returns `AggregateFunction` instead of `ColumnOperation`
   - Blocks: `test_groupby.py::test_group_by`
   - Blocks: All `agg()` operations with convenience methods

2. **BUG-002**: Aggregate functions return `AggregateFunction` instead of `ColumnOperation`
   - Blocks: All aggregation tests in `test_aggregations.py`
   - Blocks: All aggregation function tests

See `tests/BUG_LOG.md` for detailed bug information.

## Migration Strategy

### Phase 1: Core Operations ✅ (Complete)
- Basic dataframe operations
- Core functions
- Basic SQL queries

### Phase 2: Extended Functions 🔄 (In Progress)
- Extended string/math/array functions
- Conditional and special functions
- Type and conversion functions

### Phase 3: Advanced Features ⏳ (Pending)
- Complex operations
- Chained operations
- Edge cases

### Phase 4: Cleanup ⏳ (Pending)
- Remove old test files
- Update documentation
- Update CI/CD

## Files Created

### Test Files (16 files)
1. `tests/parity/dataframe/test_select.py`
2. `tests/parity/dataframe/test_filter.py`
3. `tests/parity/dataframe/test_transformations.py`
4. `tests/parity/dataframe/test_join.py`
5. `tests/parity/dataframe/test_set_operations.py`
6. `tests/parity/dataframe/test_window.py`
7. `tests/parity/dataframe/test_aggregations.py`
8. `tests/parity/dataframe/test_groupby.py`
9. `tests/parity/functions/test_string.py`
10. `tests/parity/functions/test_math.py`
11. `tests/parity/functions/test_aggregate.py`
12. `tests/parity/functions/test_array.py`
13. `tests/parity/functions/test_datetime.py`
14. `tests/parity/functions/test_null_handling.py`
15. `tests/parity/sql/test_queries.py`
16. `tests/parity/internal/test_session.py`

### Infrastructure Files
1. `tests/fixtures/parity_base.py` - Base class for all parity tests
2. `tests/PARITY_TESTING_GUIDE.md` - Comprehensive testing guide
3. `tests/parity/MIGRATION_STATUS.md` - This file
5. `tests/parity/README.md` - Quick reference for parity tests

## Next Steps

1. Continue migrating extended function tests
2. Verify all migrated tests pass (`make test-parity-phases`)
3. Run full `pytest tests -n 10` before release (see `docs/TESTING_GUIDE.md` § Maintainer gate)
4. Generate missing expected outputs
5. Begin cleanup of old test directories

