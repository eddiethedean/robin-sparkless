# Archived Tests

This directory contains archived test directories that have been superseded by the unified parity testing architecture in `tests/parity/`.

## Archive Date

**2025-01-15**

## Archived Directories

### `unit/`
Original unit tests (65+ files). These have been migrated to `tests/parity/` where PySpark equivalents exist, or remain here if they test Sparkless-specific internals without PySpark equivalents.

### `compatibility/`
Original compatibility tests (37 files) using expected outputs. These have been fully migrated to `tests/parity/` with improved organization and structure.

### `api_parity/`
Original API parity tests (7 files) using direct runtime comparison. These have been migrated to `tests/parity/` using the expected outputs pattern.

## Migration Status

All tests have been reviewed and migrated to the new unified structure:
- **Migrated**: Tests with PySpark equivalents → `tests/parity/`
- **Archived**: Tests without PySpark equivalents or superseded by parity tests

## New Test Structure

All active tests are now in `tests/parity/` organized by feature:
- `tests/parity/dataframe/` - DataFrame operations
- `tests/parity/functions/` - SQL functions
- `tests/parity/sql/` - SQL execution
- `tests/parity/internal/` - Internal features with PySpark equivalents

## Why Archived?

1. **Unified Structure**: All PySpark parity tests are now in one location (`tests/parity/`)
2. **Consistent Pattern**: All tests use the expected outputs pattern
3. **Better Organization**: Tests organized by feature, not by test type
4. **Reduced Duplication**: Eliminated duplicate tests across unit/compatibility/api_parity

## Restoration

If needed, these tests can be restored from git history:
```bash
git checkout <commit-before-archive> -- tests/unit tests/compatibility tests/api_parity
```

## References

- `tests/PARITY_TESTING_GUIDE.md` - Guide for new parity tests
- `tests/MIGRATION_STATUS.md` - Migration progress and status
- `tests/IMPLEMENTATION_COMPLETE.md` - Implementation summary

