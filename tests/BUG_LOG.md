# Bug log (parity migration)

Historical notes for the pytest parity migration under `tests/parity/`. For current engine bugs, use GitHub issues and `docs/TEST_FAILURE_CHECKLIST.md`.

## Resolved (examples)

- **BUG-001** — `GroupedData.count()` return type vs PySpark: fixed; groupBy/agg tests pass in the main suite.
- **BUG-002** — Aggregate functions return type: fixed; aggregation tests run under sparkless mode.

## Current baseline (May 2026)

Full suite: `pytest tests -n 12` → **3115 passed, 64 skipped**. See [TESTING_GUIDE.md](../docs/TESTING_GUIDE.md#expected-skips) for skip inventory.
