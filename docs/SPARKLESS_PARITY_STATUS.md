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
| Hand-written (`tests/fixtures/*.json`) | — | 88 | 0 | 0 |
| Sparkless converted (`tests/fixtures/converted/*.json`) | 0 (run converter when `SPARKLESS_EXPECTED_OUTPUTS` set) | — | — | — |

**Target: 50+ tests passing** (hand-written + converted). **Current: 88 passing** (hand-written). Phase 15 completed (aliases, string left/right/replace/like/ilike/rlike, math cosh/sinh/tanh/cbrt/expm1/log1p/etc., array_distinct). **Phase 21** (Sparkless integration) target: 200+ Sparkless tests passing with robin backend (after Phase 20: publish Rust crate). CI runs parity on hand-written (and optionally converted) fixtures; when Sparkless repo is available, run `make sparkless-parity` and update this doc.

### When Sparkless repo is available

1. Set `export SPARKLESS_EXPECTED_OUTPUTS=/path/to/sparkless/tests/expected_outputs`.
2. Run `make sparkless-parity` (converts to `tests/fixtures/converted/`, then runs `cargo test pyspark_parity_fixtures`).
3. Update the table above with converted count and passing/failing/skipped for `tests/fixtures/converted/*.json`.
4. For any failing fixture, add a row under "Failure reasons" and use `skip: true` + `skip_reason` in the fixture if it is a known unsupported or semantic difference.

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
