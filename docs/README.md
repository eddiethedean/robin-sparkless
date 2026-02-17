# Robin Sparkless Documentation

This page is the **legacy doc index**. The full documentation is built with [MkDocs](https://www.mkdocs.org/) and published on [Read the Docs](https://robin-sparkless.readthedocs.io/). To build locally: `pip install -r docs/requirements.txt` then `mkdocs serve`.

| Document | Description |
|----------|-------------|
| [QUICKSTART](QUICKSTART.md) | Build, install, basic usage, optional features (SQL, Delta), troubleshooting, benchmarks |
| [ROADMAP](ROADMAP.md) | Development roadmap and Sparkless integration phases |
| [RELEASING](RELEASING.md) | How to cut a release (version bump, tag, crates.io publish) |
| [CHANGELOG](../CHANGELOG.md) | Version history and release notes |
| [PARITY_STATUS](PARITY_STATUS.md) | PySpark parity coverage matrix (159 fixtures; 3 plan fixtures; Phases 12–25 + signature alignment + gap closure) |
| [PYSPARK_DIFFERENCES](PYSPARK_DIFFERENCES.md) | Known divergences from PySpark (window, SQL, Delta, rand/randn semantics; DataFrame cube/rollup/write/saveAsTable and stubs; in-memory tables and catalog; Phase 8 + gap closure) |
| [ROBIN_SPARKLESS_MISSING](ROBIN_SPARKLESS_MISSING.md) | What Sparkless has and robin-sparkless does not (or stub only); XML/XPath/sentences deferred |
| [PYTHON_API](PYTHON_API.md) | Historical Python API contract for the now-removed in-tree PyO3 bridge; kept for reference only |
| [SIGNATURE_GAP_ANALYSIS](SIGNATURE_GAP_ANALYSIS.md) | PySpark vs robin-sparkless signature gap analysis (params, types, defaults) and recommendations |
| [SIGNATURE_ALIGNMENT_TASKS](SIGNATURE_ALIGNMENT_TASKS.md) | Checklist to align Python param names to PySpark (historical, for the previous Python bindings) |
| [CONVERTER_STATUS](CONVERTER_STATUS.md) | Sparkless → robin-sparkless fixture converter |
| [SPARKLESS_PARITY_STATUS](SPARKLESS_PARITY_STATUS.md) | Phase 5: pass/fail and failure reasons for converted fixtures |
| [FULL_BACKEND_ROADMAP](FULL_BACKEND_ROADMAP.md) | Phased plan to full Sparkless backend replacement (Phases 12–25 + gap closure; ~295+ functions, 159 fixtures, plan interpreter; Phase 26 crate publish, Phase 27 Sparkless integration) |
| [GAP_ANALYSIS_SPARKLESS_3.28](GAP_ANALYSIS_SPARKLESS_3.28.md) | Full gap analysis vs Sparkless 3.28.0 (installed API comparison) |
| [PARITY_CHECK_SPARKLESS_3.28](PARITY_CHECK_SPARKLESS_3.28.md) | Double-check parity: implemented vs gap (Feb 2026) |
| [PHASE15_GAP_LIST](PHASE15_GAP_LIST.md) | Function gap list (PYSPARK_FUNCTION_MATRIX vs robin-sparkless) |
| [SPARKLESS_INTEGRATION_ANALYSIS](SPARKLESS_INTEGRATION_ANALYSIS.md) | Sparkless backend replacement strategy, architecture, test conversion |
| [SPARKLESS_REFACTOR_PLAN](SPARKLESS_REFACTOR_PLAN.md) | Refactor plan for Sparkless (serializable logical plan) to prepare for robin backend |
| [READINESS_FOR_SPARKLESS_PLAN](READINESS_FOR_SPARKLESS_PLAN.md) | What robin-sparkless can do in parallel (plan interpreter, fixtures, API) before merge |
| [LOGICAL_PLAN_FORMAT](LOGICAL_PLAN_FORMAT.md) | Backend plan format (op list + payload shapes + expression tree) consumed by execute_plan; full expression support (all scalar functions in filter/select/withColumn) |
| [TEST_CREATION_GUIDE](TEST_CREATION_GUIDE.md) | How to add parity tests and convert Sparkless fixtures |
| [IMPLEMENTATION_STATUS](IMPLEMENTATION_STATUS.md) | Polars migration status, build & test status |
