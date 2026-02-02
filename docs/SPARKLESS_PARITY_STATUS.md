# Sparkless Parity Status (Phase 5)

This doc records **pass/fail counts and failure reasons** for fixtures converted from Sparkless `expected_outputs` (see [CONVERTER_STATUS.md](CONVERTER_STATUS.md)).

## How to run

```bash
export SPARKLESS_EXPECTED_OUTPUTS=/path/to/sparkless/tests/expected_outputs
make sparkless-parity
```

Or run parity only (hand-written + `tests/fixtures/converted/*.json`):

```bash
cargo test pyspark_parity_fixtures
```

## Pass/fail summary

| Source | Converted | Passing | Failing | Skipped |
|--------|-----------|---------|--------|---------|
| Hand-written (`tests/fixtures/*.json`) | — | 51 | 0 | 0 |
| Sparkless converted (`tests/fixtures/converted/*.json`) | 0 (run converter when `SPARKLESS_EXPECTED_OUTPUTS` set) | — | — | — |

**Target: 50+ tests passing** (hand-written + converted). **Current: 51 passing** (hand-written). When Sparkless `expected_outputs` is available, run `make sparkless-parity` to convert and run converted fixtures; update this table with converted counts.

## Failure reasons (converted fixtures)

When a converted fixture fails, classify and document here:

| Fixture name | Reason | Notes |
|--------------|--------|-------|
| (example) | unsupported: regexp_extract | robin-sparkless does not yet implement this function |
| (example) | semantic: null in groupBy key | PySpark vs Polars grouping difference |

**Reason categories:**

- **converter**: Wrong op/schema/rows from conversion; fix in `convert_sparkless_fixtures.py` or Sparkless metadata.
- **unsupported**: Missing function or expression in robin-sparkless (Phase 6).
- **semantic**: Implemented but result differs (e.g. null handling); document and optionally add `skip: true` + `skip_reason` in the fixture.

## Skipped fixtures

Fixtures with `"skip": true` in JSON are not run. List them here for visibility:

| Fixture name | skip_reason |
|--------------|-------------|
| (none yet) | — |

## Related

- [CONVERTER_STATUS.md](CONVERTER_STATUS.md) – converter usage and operation mapping
- [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md) – Phase 5 goals
