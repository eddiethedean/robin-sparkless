# Validation and release evidence

[Plan index](README.md) · [Epic gates](EPIC.md#acceptance-and-release-gates) · [Decision register](DECISIONS.md)

**Status: implementation checks in progress; no release gates below are marked passed.** Targeted native, launcher, release-inventory, and packaging checks have run locally; they do not establish release-gate conformance. Every gate evidence field remains pending until its complete scenario and required artifact are recorded. Documentation checks do not establish server conformance.

For each gate record the native commit, client/SDK version, emulator/oracle revision and digest where applicable, command, environment/hardware, result artifact, and known limitations. Compare schemas as well as values. Keep separate results for direct HTTP, SDK/emulator, standalone Connect, and emulator-proxied Connect.

## Gate matrix

| Gate | Phase / owner | Required scenario | Assertion / evidence |
| --- | --- | --- | --- |
| V01 | 0A / SS-01 | Locked compatibility/semantic manifest | Exact client/proto/oracle/runtime versions and settings; no unresolved test pins |
| V02 | 0A / SS-01 | SDK decoding of agent result mapped by emulator | Public data_array and manifest.schema retain exact values, order, nulls and empty schema; upstream fix tracked |
| V03 | 0A / SS-03 | Default-client native feasibility | Range/filter/count returns 4; empty typed result; initialization and release trace |
| V04 | 0A / SS-03 | Disconnect after a response | Client reattaches with defaults enabled; native plan is not rerun |
| V05 | 0B / SS-05 | SELECT 1 AS result | Value and schema via direct agent and public SDK through corrected pinned emulator |
| V06 | 0B / SS-05, SS-08 | BIGINT 9007199254740993 and high-precision DECIMAL | No intermediate binary64 conversion; exact value and declared logical type/precision/scale |
| V07 | 0B / SS-05, SS-08 | Typed null, empty result, date/timestamp | Correct null representation, schema on zero rows, declared units/timezone |
| V08 | 0B / SS-05 | Column order, duplicate labels, malformed and unsupported requests | Positional values survive; unsupported kind/Python/SQL/type/write cannot return success |
| V09 | 0B / SS-04, SS-05 | Input/decoded/output/individual-value/queue limits | Bounded rejection, no successful truncation, no unbounded waiting queue |
| V10 | 0B / SS-04 | Saturated native work plus health traffic | Health progresses; timed-out work keeps permit until producer actually terminates |
| V11 | 0B / SS-07 | Session TTL/close during active work | Session isolation matches declared scope; active references and shared ownership remain safe |
| V12 | 0B / SS-06 | Sync/async launcher normal and exception exits | Owned children reaped; endpoints valid only while ready; same-object re-entry rejected |
| V13 | 0B / SS-06 | Startup cancellation, timeout/crash, noisy logs, port collision, binary mismatch, restart, nested instances | Bounded diagnostics, responsive event loop, deterministic cleanup, no orphan or unrelated-process termination |
| V14 | 0B / SS-02, SS-11 | Native packaging/platform/feature checks | Real SQL runs without Python/JVM in server image; tested platform matrix and dependency isolation |
| V15 | 1 / SS-07 | Cross-session saved tables/temp views, statement snapshots, close/TTL/restart, separate service instances | Saved data outlives sessions, local shadowing stays isolated, pinned dropped versions remain safe, ephemeral restart is explicit; no global view leakage |
| V16 | 1 / SS-04, SS-07 | Serialized DDL/append, duplicate/conflicting IDs, status after close, commit/disconnect/deadline races, capacity and restart | One effect per accepted same-instance identity; atomic publication/receipt, no false rollback, bounded records without eviction; direct HTTP plus corrected public SDK/emulator route |
| V17 | 1–2 / SS-04, SS-08 | Large range/limit/count and expensive planning | Deferred construction, bounded range batches, aggregate count without whole-input row collection; allocation/RSS budgets and scheduler/health evidence |
| V18 | 1–2 / SS-08 | Enabled scalar/nested logical types and Arrow | Phase 1: logical/native IPC/payload agreement, migration and metadata/nullability/NTZ fidelity, HTTP scalar regressions and explicit unsupported mappings; Phase 2 additionally verifies client analysis/Arrow |
| V19 | 2 / SS-09 | Enabled relations/expressions/config/analysis | Unmodified client and matched reference semantics; unsupported nodes/config/artifacts fail clearly |
| V20 | 2 / SS-10 | Multi-batch and empty streams | Correct schema, offsets, IDs, completion markers, and batch byte bounds |
| V21 | 2 / SS-10 | Execute/reattach/release/interrupt/session-release races | No reexecution; prefix/all release is idempotent; restarted/lost sessions cannot silently resume as new executions |
| V22 | 2 / SS-10 | Detached operations, replay expiry/capacity pressure | Finite retention; explicit failure/expiry instead of silent eviction; permits/resources eventually reclaimed |
| V23 | 2 / SS-09, SS-10 | Multiple real client processes | Independent results and forward progress without server-side Python/JVM; clean cancellation/shutdown |
| V24 | 2 / SS-06 | Public spark_connect_url | Exposed only for a ready endpoint whose supported client suite passed |
| V25 | 2 / SS-11 | Emulator gRPC route after standalone suite passes | h2c/auth/cluster-session/metadata/streaming/reconnect correctness through actual proxy |
| V26 | Release / SS-11 | 1/2/4/8+ client benchmarks | Correct outputs, reproducible commands, throughput/latency/RSS/CPU and hardware/version evidence |
| V27 | 3 / SS-12 | Optional Python worker Jobs/notebooks and failures | Isolation, redaction, quotas, process-tree cleanup; disabled core remains Python-free |
| V28 | 4 / SS-13 | Native Delta/UC writes and independent reads | Schema/decimal/transaction/recovery validation; explicit unsupported/conflict outcomes |
| V29 | 0B / SS-02, SS-06, SS-11 | Complete 4.16.0 candidate inventory and isolated consumer installs | Parser/server/binary-extra and existing packages align; archives/wheels install without checkout paths or inherited root Cargo patches; experimental gRPC absent from normal artifacts |
| V30 | 4.16 release completion / SS-11 | Public registry/binary/container verification after publication | Fresh exact-version installs and functional witnesses pass; hashes/digests, binary metadata and every inventory item match 4.16.0 |
| V31 | 4.17 candidate / SS-02, SS-06, SS-11 | Complete 4.17.0 inventory, isolated consumer installs and prior applicable regressions plus V15–V18 | All packages/dependencies/binaries align, launcher and enabled capabilities match; no checkout paths/root Cargo patches; SDK mutation adapter pinned and tested; unsupported gRPC absent |
| V32 | 4.17 release completion / SS-11 | Public registry/binary/container verification after publication | Fresh exact-version installs exercise shared catalog writes/reads and owned launcher; complete inventory, hashes/digests and metadata match 4.17.0 |

## 4.16 execution order

Follow [416-01 through 416-09](RELEASE_4_16.md#ordered-delivery-slices). V01–V04 establish Phase 0A, V05–V14 establish the integrated HTTP/launcher surface, and V29 checks the complete packaged candidate. These must pass before the release is ready to publish. V30 runs after publication and establishes release completion. Keep the SDK/emulator and experimental Connect traces separate from the public HTTP-only capability claim.

The exact SQL golden corpus, proposed resource defaults, and negative cases are in the [4.16 implementation plan](RELEASE_4_16.md). Boundary tests should use smaller configured limits for determinism as well as confirming the selected production defaults; a request that times out must not release its live compute lease.

## 4.17 execution order

Follow [417-01 through 417-08](RELEASE_4_17.md#ordered-delivery-slices). Complete the 4.16 prerequisite and rerun applicable regression gates on the 4.17 candidate. V15/V16 establish shared ownership, statement snapshots, coordinated mutations, bounded identity/status retention, and direct/SDK outcome behavior. V17 and the Phase 1 portion of V18 establish native execution/schema changes; they do not certify production Connect.

Use the [4.17 validation corpus](RELEASE_4_17.md#validation-corpus-and-release-gates), including controlled commit races, full retention/catalog budgets, session close and instance restart, unsupported mutation rejection with zero effects, and measured range/count allocation. Record exact workload sizes, settings, native/adapter revisions, and numeric regression budgets before passing a gate. Nested schema preservation alone does not establish nested value/transport support.

V31 checks the complete 4.17.0 candidate before tagging/publication; V32 establishes completion after fresh public installs. A passing direct mutation test cannot substitute for the external adapter/public SDK gate. All evidence remains pending until implementation runs these checks.

## Test implementation guidance

Use native unit/integration tests for schema mappings, admission ownership, state transitions, and error propagation. Add property/fuzz tests where malformed nested protocols or state-machine races justify them. Integration tests must use real engine computation, not programmed success responses.

Use separate client and server environments to demonstrate runtime independence. The launcher/SDK/Connect Python environment is allowed to contain Python; the native service image must not require it. Reference Spark may use Java in its own oracle environment, which does not invalidate a separate JVM-free native service/client witness.

Inspect actual public client outputs and transport traces. Do not patch SDK models, parse result.text as a substitute for normal SDK data, disable client reattachment, or accept schema-less success as conformance.

For concurrency/cancellation, use controlled blocking/safe-point scenarios to establish ownership semantics and real workloads to establish integration behavior. A response deadline is not proof that compute stopped. Benchmark memory includes intermediates and replay buffers.

## Partial implementation checks

These are local component checks, not passing release gates. The current work has run strict server clippy, server control-protocol integration, Polars SQL unit/integration tests, Python launcher tests, release-inventory tests, Ruff, mypy, and a locally built/installed macOS arm64 server wheel. Record exact final command output, source commit, platform, and artifacts when adding evidence; do not infer V12–V18 or V29–V32 from these checks.

- `cargo test --release --locked -p sparkless-server -p robin-sparkless-polars --features sql` — passed on macOS arm64, including 323 Polars library tests, 6 server library tests, the `control_protocol` and `phase1_http` integration tests, and 7 doctests.
- `cargo test --release --locked -p robin-sparkless-polars --features sql --lib shared_catalog_charges -- --nocapture` — both catalog accounting tests passed. Replacing a saved table while a statement lease is active keeps the previous version charged and blocks further growth until the last lease ends; materialized temporary-view snapshots consume catalog budget and release it on drop.
- `cargo test --release --locked -p robin-sparkless-polars --features sql --lib shared_catalog_temp_view_replacement_reserves_a_pinned_version -- --nocapture` — passed. Replacing a materialized temporary view while a reader lease is active now reserves both the old pinned snapshot and staged replacement; over-budget replacement is rejected without changing the view, then succeeds once the lease releases the old snapshot. This is a focused partial V16 catalog-capacity witness.
- `cargo test --release --locked -p sparkless-server --test phase1_http -- --nocapture` — passed after adding strict SQL mutation conversion checks. The live flow verifies INT/SMALLINT/TIMESTAMP/DATE schemas, signed-integer boundaries, invalid date and select-cast rejection without effects, shared saved-table reads, session-local view snapshots, duplicate replay/conflict/status, and restart isolation. This remains a focused partial V15/V16/V18 witness.
- `cargo test --locked -p sparkless-server --lib waiting_writer_does_not_consume_compute_admission -- --nocapture` — passed. A writer waiting on the async catalog lane leaves the sole compute permit available. This verifies queue placement only; a saturated long-query/health-progress integration witness remains open.
- `cargo test --locked -p sparkless-server --lib queued_mutation_keeps_reads_and_health_responsive -- --nocapture` — passed over TCP/HTTP with one compute slot. A mutation held behind the writer lane did not block a read or `/health`, then committed after the lane was released. Long-running saturated query health progress remains open.
- `cargo test --release --locked -p robin-sparkless-polars --features sql --lib filtered_large_range_count_uses_bounded_batches -- --nocapture` — passed on macOS 26.5.2 arm64 / Apple M4 Pro. The witness checks filtered counts at 10,000, 1,000,000, and 10,000,000 rows, reads schema before action, and verifies the 10M scan uses batches no larger than 16,384 rows; `id > 5` returns 9,999,994. Three sequential runs of `/usr/bin/time -l target/release/deps/robin_sparkless_polars-a19733dd6f0341e7 dataframe::tests::filtered_large_range_count_uses_bounded_batches --exact --nocapture` reported maximum resident set size of 19,431,424–19,496,960 bytes and peak memory footprint of 7,717,344–7,782,880 bytes, taking 0.08–0.09 seconds. These include process/test startup and are one-host measurements without a recorded baseline; numeric regression budget and scheduler/health saturation evidence remain open.
- `cargo test --release --locked -p robin-sparkless-polars --features sql --lib range -- --nocapture` — five range tests passed, including positive/negative steps, empty and zero-step ranges, endpoint values adjacent to `i64::MIN`/`i64::MAX`, the `u64::MAX` cardinality boundary, chained filter/limit semantics, bounded batches, and cancellation between batches.
- `cargo test --release --locked -p robin-sparkless-polars --features sql nested_array_ipc_roundtrip_preserves_values_and_logical_schema -- --nocapture` — passed. A Polars Arrow IPC stream round-tripped an array column containing a null element and a null row while carrying the logical `ArrayWithNullability` descriptor and field metadata. This is native nested IPC evidence; nested HTTP values remain disabled and cross-client interoperability is open.
- `cargo test --release --locked -p robin-sparkless-polars --features sql nested_struct_ipc_roundtrip_preserves_values_nulls_and_logical_schema -- --nocapture` — passed. A Polars Arrow IPC stream round-tripped nested struct values including a null outer struct while preserving nested field nullability and metadata in the logical-schema descriptor. This is a focused native struct witness; full nested transformation coverage and cross-client interoperability remain open.
- `cargo test --release --locked -p robin-sparkless-polars --features sql --lib checked_struct_mapping_keeps_lossy_nested_declarations_in_the_sidecar -- --nocapture` — passed. The checked plain-Polars schema path rejects nested struct declarations that would lose field nullability or metadata, including structs inside arrays; the explicit physical mapping remains available when a logical-schema sidecar accompanies it. Physical maps remain rejected.
- `cargo test --release --locked -p sparkless-server mutation_retention_capacity_rejects_before_catalog_effects -- --nocapture` — passed. Undersized retention configuration is rejected; with one record retained, a second mutation gets HTTP 429 before creating its database, while status for the first committed operation remains available. This is a focused partial V16 boundary witness, not complete gate evidence.
- `cargo test --release --locked -p sparkless-server --test phase1_http -- --nocapture` — passed on macOS arm64; the latest run completed in 24.14 seconds. It launches the native server twice and exercises shared saved-table reads, session-local temporary-view isolation and snapshot pinning, duplicate replay/conflict/status, restart isolation, scalar DDL schemas, integer/short boundaries, and rejected invalid date/cast mutations with zero catalog effects. It is focused partial V15/V16/V18 evidence, not complete gate evidence.
- `cargo test --release --locked -p robin-sparkless-polars --features sql --lib managed_int32_validation_is_strict_without_changing_legacy_int_aliases -- --nocapture` — passed. The managed SQL `int32` validator rejects values outside 32-bit range while the preexisting generic `int` validation behavior remains intact.
- `cargo clippy --locked -p sparkless-server --all-features --all-targets -- -D warnings` — passed after catalog-retention changes.
- `cargo clippy --locked -p robin-sparkless-polars --features sql --all-targets -- -D warnings` — passed after catalog-retention changes.
- `PYTHONPATH=python/sparkless .venv/bin/python -m pytest tests/server/test_launcher.py -q` — 5 passed.
- `PYTHONPATH=scripts .venv/bin/python -m unittest test_check_release_versions -v` — 10 passed.
- `sparkless_server_bin-4.15.3-py3-none-macosx_26_0_arm64.whl` (local SHA-256 `a77950fd7b74fe96f07e5c706c5b2ab65b56dd744f527eced639368004900b98`) was installed into a clean target directory and its packaged executable launched and stopped through the Python owner.
- The container image built with pinned builder/runtime image digests and returned HTTP 200 from `/health` as its configured non-root user. This is one local Linux witness, not a supported platform matrix.

Every advertised binary-wheel platform still requires independent build/install/runtime witnesses. CI must add targeted format/clippy/tests for supported feature combinations, launcher tests on supported Python/platform versions, and the distinct direct HTTP, SDK/emulator, standalone Connect, and emulator-proxied Connect routes. Keep their evidence separate and do not report planned commands as passed.

## Evidence record template

- Gate ID / result (pending, passed, failed, or blocked):
- Owner / implementation issue or PR:
- Native commit and enabled features:
- Client/SDK/emulator/oracle versions and digests:
- Semantic settings and resource limits:
- Platform/hardware and exact command:
- Test output / logs / measured results:
- Limitations or blocker:

A blocked emulator prerequisite leaves the integrated 0B gate incomplete even when direct HTTP passes. Optional Phase 3/4 evidence is not a prerequisite for shipping the explicitly limited Phase 0/1/2 surfaces.
