# Decision register

[Plan index](README.md) · [Epic](EPIC.md) · [Validation](VALIDATION.md)

**Status:** The standalone D02–D04/D07–D11 contract and Python 3.11 minimum are frozen for 4.17 below; final candidate and distribution evidence remain open. D05's coordinated package versions remain resolved. Future Connect/client/protocol and later-phase choices remain outside the 4.17 gate.

Do not replace an unresolved value with a guessed version or silently treat a recommendation as implemented. Record resolution date, owner, exact immutable revisions, rationale, impacted plans, and validation evidence here.

**Standalone development policy (2026-10-25):** Sparkless milestones, CI, and release readiness depend on its own native/HTTP/Connect contracts, standard clients, and self-contained fixtures. Work in external applications is managed separately; their features, maintainer decisions, and merge timing cannot block Sparkless.

## DuckDB phase placement (resolved 2026-10-25)

The project owner requested two separate phases after supported Connect: **Phase 3 / 4.19, DuckDB as a data source feeding Polars**, then **Phase 4 / 4.20, optional DuckDB execution through Connect**. [SS-14](14-duckdb-data-source.md) and [SS-15](15-duckdb-connect-backend.md) define the boundaries and delivery slices. Python workers move to Phase 5 / 4.21 and native Delta/UC integration to Phase 6 / 4.22. This resolves scheduling and scope, not the implementation choices in D14/D15 or compatibility evidence in V33–V36.

D14 must select a native source/driver and prove database ownership, stable snapshots, schema fidelity, and bounded batch transfer. D15 must separately select the execution/backend interface, Spark semantic translation, and transaction/outcome guarantees. Live shared-table access requires a supported common DuckDB owner; mounting the same database files or exporting a copy is not that proof. Default SQL/Connect remains Polars, and every DuckDB capability is optional and Python/JVM-free in the native server.

## 4.16 implementation baseline

The [4.16 implementation plan](RELEASE_4_16.md) proposes concrete choices for D02/D04 (restricted SQL, explicit server semantics, exact scalar results), D06 (owned JSON-lines control), D07/D08 (operation accounting and bounded defaults), D09 (read-only ephemeral sessions), and the HTTP scalar slice of D11. These are design baselines to verify in slices 416-01 through 416-08, not completed compatibility decisions or test evidence. Record deviations and their impact before freezing the contracts. All unrelated platform/version choices remain open; the shared 4.16.0 package-version policy is already fixed.

## 4.17 frozen standalone implementation contract (2026-10-27)

Release owner: project maintainer, implementation and evidence in [PR #1727](https://github.com/eddiethedean/robin-sparkless/pull/1727). The maintainer chose to preserve the existing Rust source API and set Python 3.11 as the minimum for this release. The staged entries below are historical; these are the implemented choices for D02–D04 and D07–D11. Gate acceptance still requires the final candidate run and the dependency-security hold to clear.

| Decision | Frozen 4.17 contract | Implementation / validation |
| --- | --- | --- |
| D02–D04 | HTTP only; agent/mutation protocol `1`, control protocol `1`; native server uses ANSI casts, UTC and case-insensitive identifiers. Supported managed DDL, atomic INSERT and local views are the explicit allowlist in `RELEASE_4_17.md`; unsupported descendants fail before effects. Scalar HTTP results remain positional and exact, with nested/BINARY/BYTE/FLOAT32/NTZ remote expansion disabled. | `validate_statement`, `execute_statement`, HTTP golden/type/rejection tests in `crates/sparkless-server/src/lib.rs` |
| D07/D10 | Request identities are scoped to the running instance and exact request/session generation/context. Same-identity replay has one effect and retains the original outcome after close or disconnect; reusing an identity with changed request bytes or another live session generation conflicts. Status lookup creates no record, restart creates an empty catalog/new identity, no eviction or durable recovery is promised. Exact retained replay bypasses saturated compute admission. | Native/direct-HTTP duplicate, conflict, deadline, close, disconnect, capacity, and restart fixtures; packaged verifier repeats effects/replay/status |
| D08 | Defaults: 2 active/16 pending operations, 64 sessions, 15-minute idle TTL, 1 MiB request/256 KiB SQL, 10k result rows/8 MiB result/1 MiB cell; 1,024 records/4 MiB retained outcomes; 128 databases/1,024 tables/128 views per session; 64 MiB table/256 MiB accounted catalog. Parsing and validation use bounded blocking compute before execution; timeouts retain ownership until their producer exits. Catalog accounting measures shared visible buffers, not RSS. | `ServerConfig`, catalog/reservation tests, admitted large rejected-SQL health witness, SHA-bound V17 resource report in candidate workflow |
| D09 | Catalog ownership is per service instance; saved tables outlive session close/TTL; temporary views/current database remain local. Reads pin one statement snapshot; writer publication is serialized and atomic; old versions stay charged while pinned. | Shared-catalog/session lifecycle, snapshot interleaving and retention-pressure tests |
| D11 | Legacy `StructField` literals, `StructType::new` and `fields()` remain source-compatible. Additive `LogicalField`, `from_logical_fields()` and `logical_fields()` retain metadata; nested descriptors use `LogicalStruct` while preserving the existing JSON `Struct` tag. Metadata is capped at 32 KiB per schema/depth 16. Checked unsupported mappings fail; native schema/IPC tests do not enable new HTTP result types. | External-style source fixture, legacy/nested JSON round trips, conversion, transformation, append, union, outer-join and Arrow IPC fixtures |
| Python distribution | Python >=3.11 for `sparkless` and `sparkless-server-bin`; CPython 3.11 ABI3 extension wheels; supported test interpreters 3.11–3.14. No 3.8–3.10 distributions/support. | Manifest/ABI policy and candidate wheel/sdist metadata checks |

External SDK/emulator acceptance and future Connect, DuckDB, durable catalog, and Python-worker work remain outside this release. V32 is a post-publication verification step; it cannot substitute for the pre-publication candidate checks.

## 4.17 implementation baseline

Historical staged record, superseded for the standalone release by the frozen 2026-10-27 contract above. The [4.17 implementation plan](RELEASE_4_17.md) originally proposed these extensions to 4.16. The native D10 fingerprint construction is recorded below at an implementation stage; the other 417-01 choices and the complete native/direct-HTTP outcome matrix remain unresolved. Component evidence does not pass the release gates.

| Decisions | Phase 1 choices to verify | Required evidence |
| --- | --- | --- |
| D02–D04 | Identifier/current-database rules, managed-table SQL allowlist, agent version, exact native HTTP mutation receipts and outcome mapping | Golden AST rejection/effect tests and native/direct-HTTP fixtures |
| D07, D10 | Instance-scoped operation keys/fingerprints; status-only lookup; commit versus transport state; process-lifetime retention without eviction. Native D10 digest construction is staged below; native/direct-HTTP outcomes remain open. | Duplicate/conflict/close/disconnect/deadline/restart tests; atomic commit/receipt boundary; no automatic retry after unknown outcomes |
| D08 | Writer scheduling; catalog/snapshot/staging budgets; proposed 1,024 records/4 MiB outcome storage; lazy execution workloads. Provisional local range budgets: construction at 10k/1m/10m rows stays below 1 MiB cumulative Rust-global allocation and 256 KiB live peak; filtered `id > 5` count at 10m stays below 1 MiB live peak and 250 ms; `LIMIT 10` stays below 16 MiB live peak; the standalone range benchmark stays below 64 MiB max RSS; the exact 1,024-request sustained HTTP lazy-count test (256 requests per client) stays below 256 MiB max RSS for its release-test process on macOS arm64. The 10m-row eager i64 replacement witness with the old version snapshot-pinned stays below 256 MiB max RSS and accounts 160,000,000 staged bytes plus 80,000,000 retained old-version bytes. | Candidate V17 measurements and the report digest are recorded in [VALIDATION](VALIDATION.md); these are regression thresholds, not hard memory bounds. The candidate Linux results do not certify the platform-specific macOS RSS budget; the allocator counter excludes native allocator overhead and RSS is measured externally. |
| D09 | Ephemeral shared catalog, isolated temp views, statement snapshots, saved data surviving close/TTL but lost on restart | Cross-session and separate-service isolation, read/write/drop races, fresh-instance rejection of old IDs |
| D11 | Logical metadata and BYTE/FLOAT32/NTZ distinctions, nested nullability, checked physical/IPC conversions, old-schema compatibility and per-transform propagation; metadata is bounded to 32 KiB per schema and depth 16. Native join outputs follow left fields by exact name, right fields by exact name or generated `_right` aliases, retain logical metadata, and mark null-extended outer-join fields nullable. `ArrayWithNullability` maps to Polars `List` for physical join casts while its nullability remains in the sidecar. | Phase 1 native schema/value matrix and HTTP scalar regressions; focused join IPC evidence is recorded in [VALIDATION](VALIDATION.md); per-type remote encoding gates remain explicit |

### D10 staged native fingerprint record

Owner remains unassigned. Recorded 2026-10-24 at source commit [`07353b9e3ea6c004afb092d34b5fc487efced774`](https://github.com/eddiethedean/robin-sparkless/commit/07353b9e3ea6c004afb092d34b5fc487efced774).

The native server computes a SHA-256 request digest over the exact UTF-8 `session`, `kind`, and SQL `code`, each preceded by its 64-bit big-endian byte length. After acquiring the per-session serial lane, before the catalog writer lane or execution, it computes an immutable semantic digest over length-prefixed agent and mutation protocol versions, the 32-byte request digest, the 64-bit big-endian session generation, and the length-prefixed current database. No SQL normalization is applied. This captures the context in which the statement will execute, including changes from an earlier queued `USE`.

Duplicate lookup intentionally uses the request digest and session ID, rejecting a generation mismatch when that session ID is live. If the session is closed, it can return the retained operation without reacquiring a session lease. Lookup does not recompute the semantic digest. The focused test verifies the semantic digest changes with database or generation and stays fixed after the session database changes. This resolves only the native digest-construction stage of D10; the full native/direct-HTTP mutation outcome/capacity matrix remains open.

D05/D06 also require the complete 4.17.0 candidate inventory, exact launcher/binary coupling, platform evidence, and public-install verification in V31/V32. Retain the independent protocol versions and coordinated package-version policy. Phase 2 replay/analysis/Arrow client decisions remain open.

## Decisions required by milestones

| ID | Decision | Required by | Responsible workstream | Resolution requirements |
| --- | --- | --- | --- | --- |
| D01 | Client/proto/oracle compatibility manifest | 0A | SS-01, SS-03 | Exact pyspark-client release, matching proto tag/commit, Python range, gRPC/protobuf/PyArrow locks, generator version, reference Spark/Sail versions and settings |
| D02 | SQL/configuration semantics | 0A | SS-01, SS-08 | Explicit engine profile, ANSI/case sensitivity/timezone/decimal behavior; mutable versus read-only configuration; unsupported keys fail |
| D03 | Optional interoperability and trust scope | Before advertising an external route | SS-01, SS-07, SS-11 | Standalone native/HTTP/Connect contracts and in-repository fixtures gate releases. External application changes, acceptance, and route certification are supplemental and nonblocking. Client-supplied identities are not authentication. |
| D04 | Supported SQL/type/JSON contract | 0A specification; 0B verification; mutation extension in 1 | SS-01, SS-05, SS-07, SS-08 | Allowlisted read-only nodes/functions, Phase 1 managed SQL/identity/status extension, logical schema JSON, exact integer/decimal/date/time/null encodings, duplicate labels, non-finite float policy |
| D05 | Binary/platform/release matrix | 0B | SS-02, SS-06, SS-11 | Staged server-binary target set: `macosx_14_0_arm64` and `manylinux_2_28_x86_64`; candidate single-architecture container: `linux/amd64`. The release workflow defines native wheel/install/start/health/shutdown checks, `auditwheel show`, container SQL/non-root/graceful-stop checks, and publication ordering. Local macOS 26 arm64 wheel smoke and linux/arm64 container smoke pass; the image runs as UID 65532, serves SQL, and exits 0 on SIGTERM. Remote candidate workflow evidence, the linux/amd64 image, complete package/platform matrix, public artifacts, and final release verification remain open. |
| D06 | Launcher control and compatibility | 0B | SS-02, SS-06 | Message framing/size, per-launch identity, endpoint readiness, version negotiation, Windows/POSIX graceful shutdown, cleanup and fork restrictions |
| D07 | Operation/transport state and retention | Prototype in 0A; production base in 0B; mutation outcomes in 1; replay in 2 | SS-03, SS-04, SS-07, SS-10 | State transitions, actual completion ownership, deadlines, stable identities, mutation outcome retention, reattach/release, expiry and restart behavior |
| D08 | Resource limits and enforcement | 0B base; catalog/outcome extension in 1; replay extension before 2 | SS-04, SS-07, SS-08, SS-10, SS-11 | Concrete defaults for sessions/active work/queues/input/decoded data/output; catalog/staging/snapshot and outcome capacity; lazy execution measurement budgets; Polars thread sizing; enforced versus cooperative budgets; replay byte/TTL limits |
| D09 | Catalog lifetime and persistence | Session slice in 0B; shared catalog in 1 | SS-07, SS-13 | Reserved session lifetime, catalog scope, temp-view isolation, storage/metadata ownership, visibility, restart durability and recovery |
| D10 | Mutation outcomes and retries | Before enabling writes in 1 | SS-07 | Operation/idempotency identity, duplicate handling, catalog locking/snapshots, commit receipts, unknown outcome and restart limits; external adapter extension |
| D11 | Logical schema and Arrow boundary | Minimal scalar path in 0B; native schema/type repairs in 1; full enabled matrix before 2 | SS-08, SS-09, SS-10 | Metadata preservation, schema migration/propagation rules, physical/logical mapping, IPC implementation/version, empty batches, byte limits, local relation decode expansion/artifact threshold |
| D12 | Python worker security and packaging | Before Phase 5 implementation | SS-12 | Explicit opt-in scope, worker API, code trust, OS isolation, quotas, credentials, environment, results, and process-tree ownership |
| D13 | Native Delta/UC compatibility | Before Phase 6 implementation | SS-13 | Library/storage/catalog versions, credentials policy, supported operations, conflict/transaction semantics and independent reader |
| D14 | DuckDB source contract and database ownership | Before Phase 3 implementation | SS-14, SS-04, SS-07, SS-08 | Exact native library/service/driver versions; standalone read-only and live-owner modes; table names/identity; multi-table transaction snapshots; Arrow schemas/batch limits; safe pushdown; source cancellation/replay; numeric resource budgets; explicit source-write rejection |
| D15 | DuckDB execution backend and Connect semantics | Before Phase 4 implementation | SS-15, SS-09, SS-10, SS-14 | Explicit engine selection and backend interface; Spark SQL/DataFrame/reference matrix; analysis/result schema agreement; transaction and receipt ownership; durable data versus replay/instance lifetime; cancellation/reattach; optional packaging/platforms; live shared-table visibility |

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
| V16 native mutation follow-up | Native candidate [`66fb57d6bfcf82ce9cd2c65817213391147c0648`](https://github.com/eddiethedean/robin-sparkless/commit/66fb57d6bfcf82ce9cd2c65817213391147c0648) | Direct HTTP verifies unknown status, 1,024-record retention, 2,050-byte minimum and 2,304-byte largest record reservation. Optional SDK evidence is preserved in [history](../../history/phase1-417-branch-evidence.md); external acceptance is nonblocking. |
| Reference Spark/Sail and settings | Unselected | Pending |
| Launcher control and HTTP envelope versions | Unselected | Pending |
| DuckDB native library, optional driver/service, Arrow boundary | Unselected | D14; record before Phase 3 implementation, including supported source ownership modes |
| DuckDB backend semantic profile and reference/client versions | Unselected | D15; separate Phase 4 backend conformance and transaction evidence |

## Fixed constraints

These are requirements, not open design choices:

- Direct native execution: Rust/Polars by default; optional DuckDB libraries/service access in Phases 3/4. No Python/JVM in native SQL/Connect execution paths.
- Loopback default; explicit deployment trust boundary for remote/container access.
- Read-only initial HTTP surface; unsupported operations return named failures.
- Exact typed HTTP results, including BIGINT values beyond binary64's exact integer range.
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
