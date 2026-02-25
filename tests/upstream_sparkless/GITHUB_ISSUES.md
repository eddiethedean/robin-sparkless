# GitHub Issues Created from BUG_LOG.md

**Created**: 2025-01-15  
**Total Issues**: 22

All bugs documented in `BUG_LOG.md` have been created as GitHub issues.

## Issue List

### Critical Issues (High Priority)

- [#2 - BUG-001](https://github.com/eddiethedean/sparkless/issues/2): GroupedData.count() returns AggregateFunction instead of ColumnOperation
- [#3 - BUG-002](https://github.com/eddiethedean/sparkless/issues/3): Aggregate functions with string arguments return AggregateFunction instead of ColumnOperation
- [#4 - BUG-003](https://github.com/eddiethedean/sparkless/issues/4): Window functions with aggregations fail due to AggregateFunction issue

### Medium Priority Issues

- [#5 - BUG-004](https://github.com/eddiethedean/sparkless/issues/5): SQL column aliases not properly parsed in SELECT statements
- [#6 - BUG-005](https://github.com/eddiethedean/sparkless/issues/6): SQL CASE WHEN expressions not properly parsed
- [#7 - BUG-006](https://github.com/eddiethedean/sparkless/issues/7): SQL HAVING clause not properly supported
- [#8 - BUG-007](https://github.com/eddiethedean/sparkless/issues/8): SQL UNION operation not properly implemented
- [#9 - BUG-008](https://github.com/eddiethedean/sparkless/issues/9): SQL subqueries not properly supported
- [#10 - BUG-009](https://github.com/eddiethedean/sparkless/issues/10): SQL LIKE clause parsing issues
- [#11 - BUG-010](https://github.com/eddiethedean/sparkless/issues/11): SQL IN clause parsing issues
- [#12 - BUG-011](https://github.com/eddiethedean/sparkless/issues/12): SQL CREATE TABLE AS SELECT not properly implemented
- [#13 - BUG-012](https://github.com/eddiethedean/sparkless/issues/13): SQL INSERT INTO statement execution order incorrect
- [#14 - BUG-013](https://github.com/eddiethedean/sparkless/issues/14): SQL UPDATE statement not properly implemented
- [#15 - BUG-014](https://github.com/eddiethedean/sparkless/issues/15): SQL INSERT INTO ... SELECT not properly implemented
- [#16 - BUG-015](https://github.com/eddiethedean/sparkless/issues/16): SQL SHOW statements return incorrect format
- [#17 - BUG-016](https://github.com/eddiethedean/sparkless/issues/17): SQL DESCRIBE statements not properly implemented
- [#18 - BUG-017](https://github.com/eddiethedean/sparkless/issues/18): Array function tests fail due to column name mismatches with expected outputs
- [#19 - BUG-018](https://github.com/eddiethedean/sparkless/issues/19): Null handling function tests fail due to column name mismatches
- [#20 - BUG-019](https://github.com/eddiethedean/sparkless/issues/20): Datetime function dayofmonth returns incorrect result
- [#21 - BUG-020](https://github.com/eddiethedean/sparkless/issues/21): Catalog.getTable with database parameter argument order incorrect
- [#22 - BUG-021](https://github.com/eddiethedean/sparkless/issues/22): SQL basic SELECT queries return wrong schema

### Low Priority Issues

- [#23 - BUG-022](https://github.com/eddiethedean/sparkless/issues/23): Inconsistent aggregate function return types

## Summary

- **Total Issues**: 22
- **Critical (High)**: 3 issues (#2, #3, #4)
- **Medium**: 18 issues (#5-#22)
- **Low**: 1 issue (#23)

All issues are labeled with `bug` and high-priority issues are also labeled with `help wanted`.

## Script

Issues were created using `tests/create_github_issues.py`. To recreate issues:

```bash
python tests/create_github_issues.py  # Creates all issues
python tests/create_github_issues.py --bug BUG-001  # Create specific bug
python tests/create_github_issues.py --dry-run  # Preview without creating
```

