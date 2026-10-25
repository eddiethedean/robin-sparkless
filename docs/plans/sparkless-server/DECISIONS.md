# Decision register

[Plan index](README.md) · [Epic](EPIC.md) · [Validation](VALIDATION.md)

**Status:** D05's coordinated package-version policy is resolved by the [4.16+ release roadmap](ROADMAP.md); a staged native D10 fingerprint record appears below. The remaining decisions, complete 4.17 SDK acceptance, package platform matrix, and release evidence remain open. All first-party packages publish together at 4.16.0, then 4.17.0, and so forth, including `spark-sql-parser`. Owners and implementation issue/PR links are unassigned. The Spark 4.0 family is an evaluation target, not a declared tested patch version.

Do not replace an unresolved value with a guessed version or silently treat a recommendation as implemented. Record resolution date, owner, exact immutable revisions, rationale, impacted plans, and validation evidence here.

## 4.16 implementation baseline

The [4.16 implementation plan](RELEASE_4_16.md) proposes concrete choices for D02/D04 (restricted SQL, explicit server semantics, exact scalar results), D06 (owned JSON-lines control), D07/D08 (operation accounting and bounded defaults), D09 (read-only ephemeral sessions), and the HTTP scalar slice of D11. These are design baselines to verify in slices 416-01 through 416-08, not completed compatibility decisions or test evidence. Record deviations and their impact before freezing the contracts. All unrelated platform/version choices remain open; the shared 4.16.0 package-version policy is already fixed.

## 4.17 implementation baseline

The [4.17 implementation plan](RELEASE_4_17.md) proposes staged extensions to the 4.16 contracts. The native D10 fingerprint construction is recorded below at an implementation stage; the other 417-01 choices and external adapter acceptance remain unresolved. Component evidence does not pass the release gates.

| Decisions | Phase 1 choices to verify | Required evidence |
| --- | --- | --- |
| D02–D04 | Identifier/current-database rules, managed-table SQL allowlist, agent version, exact mutation receipts and SDK outcome mapping | Golden AST rejection/effect tests; immutable external adapter revision and public SDK fixtures |
| D07, D10 | Instance-scoped operation keys/fingerprints; status-only lookup; commit versus transport state; process-lifetime retention without eviction. Native D10 digest construction is staged below; outcome and SDK acceptance remain open. | Duplicate/conflict/close/disconnect/deadline/restart tests; atomic commit/receipt boundary; no automatic retry after unknown outcomes |
| D08 | Writer scheduling; catalog/snapshot/staging budgets; proposed 1,024 records/4 MiB outcome storage; lazy execution workloads. Provisional local range budgets: construction at 10k/1m/10m rows stays below 1 MiB cumulative Rust-global allocation and 256 KiB live peak; filtered `id > 5` count at 10m stays below 1 MiB live peak and 250 ms; `LIMIT 10` stays below 16 MiB live peak; the standalone range benchmark stays below 64 MiB max RSS; the exact 1,024-request sustained HTTP lazy-count test (256 requests per client) stays below 256 MiB max RSS for its release-test process on macOS arm64. | Saturation and pinned-version accounting; health progress; reproducible construction/action memory measurements. These are one-host regression thresholds, not hard memory bounds or a V17 pass; the allocator counter excludes native allocator overhead and RSS is measured externally. |
| D09 | Ephemeral shared catalog, isolated temp views, statement snapshots, saved data surviving close/TTL but lost on restart | Cross-session and separate-service isolation, read/write/drop races, fresh-instance rejection of old IDs |
| D11 | Logical metadata and BYTE/FLOAT32/NTZ distinctions, nested nullability, checked physical/IPC conversions, old-schema compatibility and per-transform propagation; metadata is bounded to 32 KiB per schema and depth 16 | Phase 1 native schema/value matrix and HTTP scalar regressions; per-type remote encoding gates remain explicit |


### D10 staged native fingerprint record

Owner remains unassigned. Recorded 2026-10-24 at source commit [`07353b9e3ea6c004afb092d34b5fc487efced774`](https://github.com/eddiethedean/robin-sparkless/commit/07353b9e3ea6c004afb092d34b5fc487efced774).

The native server computes a SHA-256 request digest over the exact UTF-8 `session`, `kind`, and SQL `code`, each preceded by its 64-bit big-endian byte length. After acquiring the per-session serial lane, before the catalog writer lane or execution, it computes an immutable semantic digest over length-prefixed agent and mutation protocol versions, the 32-byte request digest, the 64-bit big-endian session generation, and the length-prefixed current database. No SQL normalization is applied. This captures the context in which the statement will execute, including changes from an earlier queued `USE`.

Duplicate lookup intentionally uses the request digest and session ID, rejecting a generation mismatch when that session ID is live. If the session is closed, it can return the retained operation without reacquiring a session lease. Lookup does not recompute the semantic digest. The focused test verifies the semantic digest changes with database or generation and stays fixed after the session database changes. This resolves only the native digest-construction stage of D10; the full mutation outcome/capacity matrix and external SDK acceptance remain open.

D05/D06 also require the complete 4.17.0 candidate inventory, exact launcher/binary coupling, platform evidence, and public-install verification in V31/V32. Retain the independent protocol versions and coordinated package-version policy. Phase 2 replay/analysis/Arrow client decisions remain open.

## Decisions required by milestones

| ID | Decision | Required by | Responsible workstream | Resolution requirements |
| --- | --- | --- | --- | --- |
| D01 | Client/proto/oracle compatibility manifest | 0A | SS-01, SS-03 | Exact pyspark-client release, matching proto tag/commit, Python range, gRPC/protobuf/PyArrow locks, generator version, reference Spark/Sail versions and settings |
| D02 | SQL/configuration semantics | 0A | SS-01, SS-08 | Explicit engine profile, ANSI/case sensitivity/timezone/decimal behavior; mutable versus read-only configuration; unsupported keys fail |
| D03 | Emulator prerequisite and trust scope | 0A tracking; 0B integration | SS-01, SS-07 | MiniLake adapter source revisions `155cb95cd49ae476ea4392dd9b5c2f63e6341fbb`, `066a58b6aab802ad1cdaefee4de0d96a39cfe4d4`, `fdd192854c6b2fe322167751c32e50ee43e6edf5`, `f015df957d1296063f004f7dfdfcc250ba22c596`, `30d49af7524bb535b76726668ac77f401953be03`, `7a8d54e69bb4eba3ee02404164e0081c5b1b7930`, `5c56cfdd5f6a772c8fd56d9dd0d87e5cdf821f63`, and `173abd57195243f6a93a434ffec46bf185d1a0d7`, `d3e85e586598129487d9b83c988d2f3295d61de8`, `b9dede13b7d5107b4c9f79de2a7f162888bf5ccb`, SDK lock `0.133.0`, typed-result, mutation-route, duplicate replay, changed-SQL/session conflict, concurrent replay, committed-response disconnect and post-timeout recovery, stale-instance rejection, real process-restart, pending-before-acceptance, pending/committed deadline recovery, accepted-before-completion pending status through the public SDK, and public-route capacity failure (HTTP 429 with no append effect) tests are recorded. Native commit `7b6197aa276702e3bfd3a0fc8dcd662c2744d1be` adds a direct HTTP deadline/status witness; upstream owner/issue or PR, image digest if used, and shared warehouse/session limitations remain tracked |
| D04 | Supported SQL/type/JSON contract | 0A specification; 0B verification; mutation extension in 1 | SS-01, SS-05, SS-07, SS-08 | Allowlisted read-only nodes/functions, Phase 1 managed SQL/identity/status extension, logical schema JSON, exact integer/decimal/date/time/null encodings, duplicate labels, non-finite float policy |
| D05 | Binary/platform/release matrix | 0B | SS-02, SS-06, SS-11 | Staged server-binary target set: `macosx_14_0_arm64` and `manylinux_2_28_x86_64`; candidate single-architecture container: `linux/amd64`. The release workflow defines native wheel/install/start/health/shutdown checks, `auditwheel show`, container SQL/non-root/graceful-stop checks, and publication ordering. Local macOS 26 arm64 wheel smoke and linux/arm64 container smoke pass; the image runs as UID 65532, serves SQL, and exits 0 on SIGTERM. Remote candidate workflow evidence, the linux/amd64 image, complete package/platform matrix, public artifacts, and final release verification remain open. |
| D06 | Launcher control and compatibility | 0B | SS-02, SS-06 | Message framing/size, per-launch identity, endpoint readiness, version negotiation, Windows/POSIX graceful shutdown, cleanup and fork restrictions |
| D07 | Operation/transport state and retention | Prototype in 0A; production base in 0B; mutation outcomes in 1; replay in 2 | SS-03, SS-04, SS-07, SS-10 | State transitions, actual completion ownership, deadlines, stable identities, mutation outcome retention, reattach/release, expiry and restart behavior |
| D08 | Resource limits and enforcement | 0B base; catalog/outcome extension in 1; replay extension before 2 | SS-04, SS-07, SS-08, SS-10, SS-11 | Concrete defaults for sessions/active work/queues/input/decoded data/output; catalog/staging/snapshot and outcome capacity; lazy execution measurement budgets; Polars thread sizing; enforced versus cooperative budgets; replay byte/TTL limits |
| D09 | Catalog lifetime and persistence | Session slice in 0B; shared catalog in 1 | SS-07, SS-13 | Reserved session lifetime, catalog scope, temp-view isolation, storage/metadata ownership, visibility, restart durability and recovery |
| D10 | Mutation outcomes and retries | Before enabling writes in 1 | SS-07 | Operation/idempotency identity, duplicate handling, catalog locking/snapshots, commit receipts, unknown outcome and restart limits; external adapter extension |
| D11 | Logical schema and Arrow boundary | Minimal scalar path in 0B; native schema/type repairs in 1; full enabled matrix before 2 | SS-08, SS-09, SS-10 | Metadata preservation, schema migration/propagation rules, physical/logical mapping, IPC implementation/version, empty batches, byte limits, local relation decode expansion/artifact threshold |
| D12 | Python worker security and packaging | Before Phase 3 implementation | SS-12 | Explicit opt-in scope, worker API, code trust, OS isolation, quotas, credentials, environment, results, and process-tree ownership |
| D13 | Native Delta/UC compatibility | Before Phase 4 implementation | SS-13 | Library/storage/catalog versions, credentials policy, supported operations, conflict/transaction semantics and independent reader |

## Required compatibility record

Populate the following together at the 0A gate; file locations may be chosen during implementation.

| Component | Exact version/revision | Lockfile/artifact/evidence |
| --- | --- | --- |
| Native engine and server source | Unselected | Pending |
| Rust toolchain/MSRV | Follow root rust-toolchain.toml and Cargo.toml; capture exact values at build | Pending build evidence |
| PySpark client and Python | Unselected | Pending |
| Spark Connect protocol sources/compiler | Unselected | Pending |
| gRPC/protobuf/PyArrow dependencies | Unselected | Pending |
| Polars/Arrow serialization boundary | Unselected | Pending |
| Databricks SDK | `databricks-sdk==0.133.0` | Locked by MiniLake `uv.lock`; public SDK integration uses the unmodified SDK models |
| Corrected emulator and image digest | MiniLake optional Sparkless adapter, commits [`155cb95cd49ae476ea4392dd9b5c2f63e6341fbb`](https://github.com/eddiethedean/minilake/commit/155cb95cd49ae476ea4392dd9b5c2f63e6341fbb), [`066a58b6aab802ad1cdaefee4de0d96a39cfe4d4`](https://github.com/eddiethedean/minilake/commit/066a58b6aab802ad1cdaefee4de0d96a39cfe4d4), [`fdd192854c6b2fe322167751c32e50ee43e6edf5`](https://github.com/eddiethedean/minilake/commit/fdd192854c6b2fe322167751c32e50ee43e6edf5), [`f015df957d1296063f004f7dfdfcc250ba22c596`](https://github.com/eddiethedean/minilake/commit/f015df957d1296063f004f7dfdfcc250ba22c596), [`30d49af7524bb535b76726668ac77f401953be03`](https://github.com/eddiethedean/minilake/commit/30d49af7524bb535b76726668ac77f401953be03), [`7a8d54e69bb4eba3ee02404164e0081c5b1b7930`](https://github.com/eddiethedean/minilake/commit/7a8d54e69bb4eba3ee02404164e0081c5b1b7930), [`5c56cfdd5f6a772c8fd56d9dd0d87e5cdf821f63`](https://github.com/eddiethedean/minilake/commit/5c56cfdd5f6a772c8fd56d9dd0d87e5cdf821f63), and [`173abd57195243f6a93a434ffec46bf185d1a0d7`](https://github.com/eddiethedean/minilake/commit/173abd57195243f6a93a434ffec46bf185d1a0d7), [`d3e85e586598129487d9b83c988d2f3295d61de8`](https://github.com/eddiethedean/minilake/commit/d3e85e586598129487d9b83c988d2f3295d61de8), [`b9dede13b7d5107b4c9f79de2a7f162888bf5ccb`](https://github.com/eddiethedean/minilake/commit/b9dede13b7d5107b4c9f79de2a7f162888bf5ccb) | Pushed to the fork branch `sparkless-phase1-adapter`; source-level live integration was used, no image digest; upstream PR/merge and owner tracking remain open. Cross-request replay requires both operation ID and expected server-instance ID; same-instance replay, changed-SQL/session conflict, concurrent same-identity replay, committed-response disconnect and post-timeout status recovery, stale-instance rejection, a real server-restart retry, pending-before-acceptance recovery, pending/committed deadline recovery, accepted-before-completion pending status through the public SDK, and HTTP 429/no-effect mutation-retention capacity handling are tested. |
| Reference Spark/Sail and settings | Unselected | Pending |
| Launcher control and HTTP envelope versions | Unselected | Pending |

## Fixed constraints

These are requirements, not open design choices:

- Direct native Rust execution; no Python/JVM in default SQL/Connect service paths.
- Loopback default; explicit deployment trust boundary for remote/container access.
- Read-only initial HTTP surface; unsupported operations return named failures.
- Exact typed SDK results, including BIGINT values beyond binary64's exact integer range.
- Unmodified Connect client defaults, including reattachment, in conformance tests.
- Native work retains admission/accounting until it actually finishes.
- No hard in-process per-query CPU/RSS or durable exactly-once promise without the required implementation.
- Minimum session and scalar/schema work ships with 0B.
- Standalone Connect conformance precedes proxy certification.

## Resolution record template

Copy this block when a decision is resolved:

- Decision ID and date:
- Owner and implementation issue/PR:
- Selected behavior and exact versions:
- Alternatives considered and reason:
- Affected contract/plan documents:
- Tests, immutable source revision, and evidence:
- Remaining limitations:

A decision can have staged resolutions (for example, D07's feasibility and production stages). Record each stage; do not mark later stages complete because the prototype passed.
