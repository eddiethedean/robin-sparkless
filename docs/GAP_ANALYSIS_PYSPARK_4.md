# Gap analysis: PySpark 3.5 vs 4.1 (Sparkless 4.9.0)

**Status:** Living document for the 4.9.0 release.  
**Baseline API snapshot (3.5):** [pyspark_api_from_repo.json](pyspark_api_from_repo.json) (branch `v3.5.0`)  
**Detailed breaking-change catalog:** [PYSPARK_4_PARITY_PLAN.md](PYSPARK_4_PARITY_PLAN.md) §5

## Summary

| Area | PySpark 3.5 oracle today | PySpark 4.1 target (4.9.0) |
|------|------------------------|----------------------------|
| Default semantics | Tier A (`compat=3.5`) | Opt-in Tier B (`compat=4.0`) |
| Parity fixtures | 212+ vs 3.5 | Expand with `pyspark4_only` / ANSI pack |
| Signature baseline | [SIGNATURE_GAP_ANALYSIS.md](SIGNATURE_GAP_ANALYSIS.md) (3.5) | Re-run vs 4.1 (P2) |
| Full API extraction | `scripts/extract_pyspark_api_from_repo.py` @ v3.5.0 | Re-run @ v4.1.x → `docs/pyspark_api_4.x.json` |

## HIGH priority (4.9.0 scope)

Tracked in implementation; see §5.1–5.2 of [PYSPARK_4_PARITY_PLAN.md](PYSPARK_4_PARITY_PLAN.md):

| Change | Sparkless 4.9.0 |
|--------|-----------------|
| ANSI default (`spark.sql.ansi.enabled`) | Profile bundle + engine (WS2) |
| Map key `-0.0` normalization | WS3 |
| Map/array schema inference | WS3 |
| `try_*` under ANSI-on | WS2 (existing `try_*` + tests) |
| PyArrow `createDataFrame` | WS4 (partial support in PyO3) |
| Interval `collect()` shapes | WS3 — `YearMonthIntervalType`, `DayTimeIntervalType`, `PYSPARK_YM_INTERVAL_LEGACY` |
| `parse_json` / VARIANT (subset) | WS4 — `VariantType`, `parse_json`, cast to variant; full semi-structured SQL deferred |

## Deferred (post-4.9.0 or optional)

| Area | Notes |
|------|--------|
| VARIANT type + SQL | Partial — `parse_json`/cast; full semi-structured SQL in [DEFERRED_SCOPE.md](DEFERRED_SCOPE.md) |
| Full JDBC 4.0 per-DB mappings | WS5 — profile + legacy flags; Postgres/MySQL/MSSQL v4 read paths |
| `mergeInto`, `groupingSets`, lateral joins | [ROBIN_SPARKLESS_MISSING.md](ROBIN_SPARKLESS_MISSING.md) |
| `pyspark.pandas` | Out of scope |
| orc/text IO, catalog DDL | [FULL_PARITY_ROADMAP.md](FULL_PARITY_ROADMAP.md) |

## Regenerating the 4.1 API snapshot

When network/git access to Apache Spark is available:

```bash
python scripts/extract_pyspark_api_from_repo.py --clone --branch v4.1.1
python scripts/gap_analysis_pyspark_repo.py --write-md docs/GAP_ANALYSIS_PYSPARK_4.md
```

Until then, use the authoritative migration sources listed in [PYSPARK_4_PARITY_PLAN.md](PYSPARK_4_PARITY_PLAN.md) §4.
