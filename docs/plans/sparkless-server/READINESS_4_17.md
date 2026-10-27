# 4.17.0 release readiness review

**Verdict: HOLD. Do not tag or publish 4.17.0.**

Reviewed `codex/release-4.17` at `a017821c2c64c513640d5428f94bd813cccaf24f` on 2026-10-26 (2026-10-27 UTC). This review covers the complete Phase 1 delivery slices, applicable 4.16 regressions, public Rust dependency behavior, Python/source/server packaging, resource ownership, and V31/V32 publication checks. External application acceptance and Phase 2 Connect certification are outside the 4.17 gate.

The runtime regression results are strong, but six actionable findings and two release-evidence requirements prevent approval. The machine-readable observations are in [the review evidence](evidence/4.17-readiness-2026-10-26.json). Production code and workflow behavior were not changed during this review.

## Actionable findings

### R1 — P1: published Rust consumers still receive vulnerable compression dependencies

The fixed workspace depends on the root `[patch.crates-io]` entries in [Cargo.toml](../../../Cargo.toml#L57). The backend still requests registry Polars 0.53 in its [published dependencies](../../../crates/robin-sparkless-polars/Cargo.toml#L38). Packaging correctly removes root patches and local dependency paths, so a passing workspace audit does not establish downstream safety.

Reproduction: copy the first-party workspace into a fresh directory, remove the root patch table, resolve with `cargo metadata --offline --all-features`, then run `cargo audit --no-fetch --file <consumer>/Cargo.lock --json` from the original checkout to apply its existing advisory policy. This reports three non-ignored vulnerabilities:

| Advisory | Resolved package | Required fixed release |
| --- | --- | --- |
| RUSTSEC-2026-0391 | zstd 0.13.3 | zstd >=0.14.3 |
| RUSTSEC-2026-0403 | zstd-safe 7.3.0 | zstd-safe >=8.2.0 |
| RUSTSEC-2026-0398 | zstd-safe 7.3.0 | zstd-safe >=8.2.0 |

The freshly generated `sparkless-server-4.17.0.crate` and facade archive actually contain zstd 0.13.3 / zstd-safe 7.3.0 in their packaged Cargo.lock files. This corroborates the isolated-consumer result directly from the distributable archives. Auditing the packaged server lockfile also identifies the same three advisories; it additionally warns that the first-party 4.17 versions are not yet in the registry index.

`cargo tree --locked --offline -p sparkless-server -i zstd@0.13.3 --edges normal` confirms this is in the normal server dependency path through Polars Arrow/Parquet, not just a test or optional Delta path. Direct zstd 0.14.3 development dependencies do not override the incompatible 0.13 requirement. The isolated consumer also resolves Apache Parquet 57.2.0/58.4.0; the checkout's fixed vendor graph is a different graph.

Closure: use fixed published upstream dependencies or a distributable fork graph, audit its normal and advertised optional consumer configurations, and verify isolated exact-version builds without root patches. Add that consumer-graph check to the publication prerequisites. Archive manifest normalization alone is insufficient. [Polars PR 30316](https://github.com/pola-rs/polars/pull/30316) remains open at review time; local source builds already work independently of it.

### R2 — P1: StructField breaks the 4.16.1 Rust source API in a minor release

[StructField.metadata](../../../crates/robin-sparkless-core/src/schema.rs#L44) adds a required public field to a struct that previously supported three-field literals. Serde defaults preserve old JSON but cannot preserve Rust source compatibility.

This exact consumer compiles against the `v4.16.1` core sources and fails against the candidate with `E0063: missing field metadata in initializer of StructField`:

```rust
use robin_sparkless_core::schema::{DataType, StructField};

fn main() {
    let _field = StructField {
        name: "id".into(),
        data_type: DataType::Long,
        nullable: false,
    };
}
```

Closure: preserve the existing public struct shape and introduce metadata through an additive descriptor/API, or explicitly move the incompatible API to an appropriately versioned major release. Add an external consumer compile fixture against the accepted baseline; existing in-repository constructors and JSON migration tests cannot catch this break. This is the public-construction compatibility surface explicitly called out by 417-05.

### R3 — P2: SQL parsing and AST validation bypass compute admission

[handle_statements](../../../crates/sparkless-server/src/lib.rs#L861) calls `validate_statement`, including full SQL parsing and AST validation, synchronously on the Tokio worker before reserving admission or reaching `spawn_blocking`. Large rejected requests therefore consume runtime CPU even when the configured operation queue is full. Existing lazy-count health tests exercise execution after admission and do not cover this path.

A local probe used 24 concurrent 156,906-byte SELECT statements ending in an unsupported function, below the default 256 KiB SQL limit. With one Tokio worker, one active operation, and one pending operation, every request returned `UnsupportedSql`; maximum health latency increased from 0.0105 seconds at baseline to 0.5331 seconds under load. This is one host observation, not a production latency bound or a claim that health stopped completely. The code placement independently demonstrates the admission gap.

Closure: run parsing/validation under bounded admission in a blocking compute context, preserve duplicate/status lookup behavior, and add a large-query planning/rejection health witness with a recorded latency budget. Until then, the 417-06 audit requirement that expensive SQL parsing/planning obey SS-04 ownership is incomplete.

### R4 — P1: candidate packaged installs do not exercise Phase 1 mutations

The [server-extra candidate check](../../../.github/workflows/release.yml#L382) calls `verify_server_extra_install.py` without `--mutations`. It verifies readiness and SELECT 1. The native mutation suite and five verifier regression tests use a workspace-built binary; neither verifies the bytes installed from the candidate distribution. This review separately built a production-feature server wheel and installed the local wheel pair offline on Python 3.8/3.11; both passed the mutation verifier. That local witness does not add the missing CI gate or certify the complete advertised platform inventory. Native wheel smoke checks also stop after readiness/health. The first packaged mutation check in the release workflow is in [V32](../../../.github/workflows/release.yml#L1187), after publication.

Closure: run the existing `--mutations` checks against fresh candidate installs before publication and preserve that evidence for each advertised server wheel, with equivalent catalog/outcome checks for the candidate image. Retain the native race suite. V32 should repeat the checks on public bytes rather than serve as their first packaged witness.

### R5 — P2: V32 never functionally exercises the crates.io-installed server

The [Rust public-install step](../../../.github/workflows/release.yml#L1139) installs the binary and compares `--version`, then proceeds to tests of the PyPI binary and container. Those artifacts use root patches; the Rust consumer graph does not. A correct version string cannot establish that the published Rust binary serves the promised SQL/schema/mutation contract.

Closure: launch the exact `$RUNNER_TEMP/public-rust/bin/sparkless-server`, verify health, typed SQL and Phase 1 effects/replay/status, then stop it gracefully. Record that binary's version and execution evidence independently from the PyPI wheel and container.

### R6 — P1: the new source backend cannot load on supported Python 3.8

The custom [PEP 517 backend](../../../python/_sparkless_build.py#L66) uses parenthesized multiple-context-manager statements at lines 66, 106, and 129. Running the actual installed CPython 3.8.20 interpreter against the reviewed file fails at line 67:

```text
SyntaxError: invalid syntax
    tarfile.open(archive_path, "r:gz") as original,
                                       ^
```

Reproduction: `python3.8 -c 'from pathlib import Path; p=Path("python/_sparkless_build.py"); compile(p.read_text(), str(p), "exec")'`. The same file compiles on Python 3.9 and 3.11. `requires-python = ">=3.8"` remains declared, so Python 3.8 source consumers cannot import the backend to build a wheel. Direct maturin wheel jobs bypass the backend, and the new source CI job selects Python 3.11; both can pass while this advertised source-install path fails. The earlier Python 3.11 AST feature-version check did not detect this grammar incompatibility.

Closure: use context-manager syntax supported by Python 3.8, and test backend loading with the actual minimum interpreter plus a current interpreter. Exercise the PEP 517 source path on that minimum version. Retain 3.8 support unless the project deliberately changes its advertised Python contract. This issue was introduced by the preceding source-packaging fix and remains unfixed in the reviewed commit.

## Release-evidence requirements

**C1 — freeze and accept the Phase 1 contracts/matrices.** [DECISIONS](DECISIONS.md#417-implementation-baseline) still labels the 417-01 choices unresolved and D10 staged. Complete D02–D04/D07–D11 with the implemented SQL/type/error/identity/limit/lifetime contracts, and map accepted V15/V16/V18 scenarios to immutable candidate evidence. The passing tests below establish the named component results; they do not turn unresolved contract records into an accepted integrated gate. Future Connect/client/proto choices and external route acceptance must not be added as 4.17 prerequisites.

**C2 — run the complete non-publishing matrix on the final candidate.** At review time, origin still points to `e3f85cb447392bba185f11dd87a32c56cd8658e1`; reviewed head `a017821c` has not been pushed and has no candidate run. [CI 40428106702](https://github.com/eddiethedean/robin-sparkless/actions/runs/40428106702) succeeded for the predecessor. [Release 40428101113](https://github.com/eddiethedean/robin-sparkless/actions/runs/40428101113) was still running at the last captured snapshot: V17, prerequisite, Rust/Python tests, supported server wheels, offline consumer, and container passed; all eight extension-wheel/sdist matrix jobs were in progress, with V31 incomplete. The earlier [Release 40391967226](https://github.com/eddiethedean/robin-sparkless/actions/runs/40391967226) passed all 42 pre-publication jobs on `7ae807d9`; six recovery/publication/V32 jobs were skipped. None of these certifies the new source backend or current head's complete artifact set.

Closure: after the code and contract findings are addressed, push the final candidate and preserve one successful run binding the eight extension wheels, sdist, two server wheels, native linux/amd64 image, V15–V18 evidence, fixed Rust consumer graph, and fresh artifact installs to its exact SHA. Recheck current public metadata immediately before release. V32 remains an expected post-publication requirement, not a gate that must run before tagging.

## Gate assessment

| Slice / gate | Review assessment | Remaining closure |
| --- | --- | --- |
| Applicable 4.16 prerequisite | Passed accepted 4.16.1; public PyPI/crates inventory reverified; predecessor candidate packaged-install witness passed | Repeat applicable regressions on final candidate |
| 417-01 | Contracts remain staged | C1 |
| 417-02 / V15 | Native cross-session, view isolation, statement snapshots, close/TTL/restart, separate-service and pin-retention witnesses pass | C1/C2 integrated acceptance |
| 417-03–417-04 / V16 | Native duplicate/conflict/concurrency, status, failed casts, close/disconnect/deadline, capacity and restart tests pass | R4, C1/C2 |
| 417-05 / V18 | Scalar/nested schema, metadata/nullability, migration, checked unsupported mappings and IPC tests pass | R2 source compatibility, C1/C2 |
| 417-06 / V17 | Range/scheduler witnesses pass; predecessor's SHA-bound V17 report passed | R3 planning admission and C2 |
| 417-07 / V31 | Metadata and local packages/source checks pass | R1/R4/R6 and C1/C2; no current-head full platform inventory |
| 417-08 / V32 | Not run; checked public package versions/tag are absent | Pre-publication gates first, then R5 and full public verification |

## Verification performed

Environment: macOS arm64, Rust/Cargo 1.93.1. Tests used the reviewed checkout, standalone consumer directories, and a fresh extracted source archive. A target build cache was shared for source compilation; every local source dependency was checked to resolve inside the extraction.

| Check | Result |
| --- | --- |
| `cargo test --release --locked --offline --workspace --features sql,delta --lib --tests` | 625 passed across 30 targets; two intentionally ignored V17 measurement tests; no failures |
| `cargo test --release --locked --offline --workspace --features sql,delta --doc` | Seven passed |
| Release verifier unit suite (`test_*release*.py`) | 52 passed |
| Standalone live HTTP verifier suite | Five passed, including deliberate bad receipt/schema/duplicate-effect/status responses |
| Owned launcher lifecycle suite | Five passed |
| `check_release_versions.py --tag v4.17.0` | All coordinated versions/lock entries/extra pins/changelog agree |
| Workspace audit with refreshed advisory database | Zero non-ignored vulnerabilities and no warnings under existing policy; 1,378 advisories at RustSec commit `3279c99911354c400e1d41e47c7d60e39258b147` |
| Isolated downstream graph audit under that policy | Fails with the three R1 advisories |
| Cargo deny advisories/bans/sources; formatting; touched Python Ruff checks | Pass |
| Cargo deny licenses | Pass with an existing missing-license-field warning for soundex 0.2.0 |
| Vendor integrity | 831 files compared against cached registry releases; only the four Cargo.toml manifests differ |
| Strict workspace/all-feature/all-target Clippy | Passed during integration of the same reviewed source tree; no production code changed in this review |
| All five Rust package archives, normalized manifest checks | Pass; this is a packaging-format result, not a downstream security pass |
| Python 3.8 backend syntax check | Fails with SyntaxError at line 67; Python 3.9/3.11 compile |
| Standard PEP 517 sdist build and fresh extracted `cargo metadata --locked` / default Python+Delta `cargo check --locked --release` | Pass; vendored patches and backend included, standalone lockfile ready |
| Full Python 3.11 wheel build from that extracted sdist | Pass, default Delta included |
| Fresh local wheel-pair installs on Python 3.8.20 and 3.11.15 | Both pass offline installation, dependency checks, documented createDataFrame/count, and the packaged HTTP mutation verifier; R6 still blocks 3.8 source builds |
| Workflow structure | 32 jobs, acyclic, all `needs` references defined |
| Public 4.17 package metadata | Both PyPI versions and all five crate versions return 404; GitHub `v4.17.0` ref returns 404; no publication attempted by this review |

The two ignored native tests are `lazy_range_construction_memory_measurement` and `shared_catalog_retention_and_staging_memory_measurement`. The V17 workflow explicitly runs its opt-in resource witnesses separately. The review did not run the complete cross-platform Python matrix, the full V17 measurement script, or rebuild the advertised linux/amd64 container locally; current-head remote evidence remains required. Existing advisory exceptions were not expanded, and passing the audit does not mean every ignored upstream advisory has been eliminated.

## Nonblocking documentation cleanup

The 4.17 changelog still describes public SDK integration as a release gate, although the workflow and standalone policy removed external SDK/MiniLake gates. Align those release notes before publication. The license policy check accepts the graph but warns that soundex 0.2.0 does not declare a license expression in its manifest; no new policy exception was introduced.

## Recommended closure order

1. Resolve R1, R2, and R6 before preparing distributable release artifacts.
2. Close R3 and extend the pre-publication/public artifact witnesses for R4/R5.
3. Freeze C1 and preserve the accepted native/direct-HTTP scenario mapping.
4. Run C2 against the final exact candidate SHA and inspect every advertised artifact.
5. After the approved release is published, complete V32 independently for crates.io, PyPI, and GHCR.
