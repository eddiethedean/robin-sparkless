# Validation and release evidence

[Plan index](README.md) · [Epic gates](EPIC.md#acceptance-and-release-gates) · [Decision register](DECISIONS.md)

**Status: required native V05–V14/V29 pre-publication gates passed for the 4.16.0 candidate.** Evidence combines the passing cross-platform GitHub Actions run, installed-consumer and Linux container checks, and the direct native acceptance tests recorded below. The 4.16 publication scope is the native HTTP service, launcher, and distributions. SDK/emulator and experimental Connect certification (V01–V04) is optional and does not block release.

For each gate record the native commit, client/SDK version, emulator/oracle revision and digest where applicable, command, environment/hardware, result artifact, and known limitations. Compare schemas as well as values. Keep separate results for direct HTTP, SDK/emulator, standalone Connect, emulator-proxied Connect, MiniLake Jobs, and optional MiniLake shared Delta/catalog/DML routes. MiniLake's DuckDB SQL success cannot prove native execution or replace an agent/proxy gate.

## Gate matrix

| Gate | Phase / owner | Required scenario | Assertion / evidence |
| --- | --- | --- | --- |
| V01 | Optional integration / SS-01 | Locked compatibility/semantic manifest | Exact client/proto/oracle/runtime versions and settings; no unresolved test pins |
| V02 | Optional integration / SS-01 | SDK decoding of agent result mapped by emulator | Public data_array and manifest.schema retain exact values, order, nulls and empty schema; upstream fix tracked |
| V03 | Optional integration / SS-03 | Default-client native feasibility | Range/filter/count returns 4; empty typed result; initialization and release trace |
| V04 | Optional integration / SS-03 | Disconnect after a response | Client reattaches with defaults enabled; native plan is not rerun |
| V05 | 0B / SS-05 | SELECT 1 AS result | Value and schema via the direct native HTTP agent |
| V06 | 0B / SS-05, SS-08 | BIGINT 9007199254740993 and high-precision DECIMAL | No intermediate binary64 conversion; exact value and declared logical type/precision/scale |
| V07 | 0B / SS-05, SS-08 | Typed null, empty result, date/timestamp | Correct null representation, schema on zero rows, declared units/timezone |
| V08 | 0B / SS-05 | Column order, duplicate labels, malformed and unsupported requests | Positional values survive; unsupported kind/Python/SQL/type/write cannot return success |
| V09 | 0B / SS-04, SS-05 | Input/decoded/output/individual-value/queue limits | Bounded rejection, no successful truncation, no unbounded waiting queue |
| V10 | 0B / SS-04 | Saturated native work plus health traffic | Health progresses; timed-out work keeps permit until producer actually terminates |
| V11 | 0B / SS-07 | Session TTL/close during active work | Session isolation matches declared scope; active references and shared ownership remain safe |
| V12 | 0B / SS-06 | Sync/async launcher normal and exception exits | Owned children reaped; endpoints valid only while ready; same-object re-entry rejected |
| V13 | 0B / SS-06 | Startup cancellation, timeout/crash, noisy logs, port collision, binary mismatch, restart, nested instances | Bounded diagnostics, responsive event loop, deterministic cleanup, no orphan or unrelated-process termination |
| V14 | 0B / SS-02, SS-11 | Native packaging/platform/feature checks | Real SQL runs without Python/JVM in server image; tested platform matrix and dependency isolation |
| V15 | 1 / SS-07 | Cross-session tables/temp views plus close/restart | Defined catalog visibility and durability; no accidental process-global view leakage |
| V16 | 1 / SS-07 | Concurrent DML/DDL, duplicate IDs, disconnect near commit, restart | No duplicate live operation effects; truthful committed/unknown/cancelled outcome within documented guarantees |
| V17 | 1–2 / SS-08 | Large range/count and expensive planning | Chosen lazy/bounded behavior verified; allocation/execution occurs under compute admission |
| V18 | 1–2 / SS-08 | Enabled scalar/nested logical types and Arrow | Logical/analysis/payload schema agree, including nullability and timestamps; unsupported types fail |
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

## MiniLake route gates (SS-14)

These are planned, independently recorded integration gates. V31–V33 apply only when advertising Phase 2 MiniLake Jobs; V34–V35 require optional Phase 4 native storage/catalog capabilities, and V36 applies only if the additional MiniLake SQL DML route is enabled. None adds a 4.16 requirement or depends on Sparkless-hosted Python workers.

| Gate | Phase / owner | Required scenario | Assertion / evidence |
| --- | --- | --- | --- |
| V31 | 2 route / SS-11, SS-14 | SDK/CLI workspace import and MiniLake Jobs → Python-only runner → native Connect | Range/filter/count returns 4; public run/output and native trace agree; unavailable server fails instead of local fallback; inspect all integration build stages/images/processes/pulls for absence of Java/JRE/JVM/Apache Spark |
| V32 | 2 route / SS-09, SS-14 | Real scripts, arguments, task env/secret references, result/error/type boundaries and unsupported packages/config | Pinned unmodified client; correct stdout/stderr/exit status and supported schemas; no secret leakage; Maven/JVM options and unsupported plans fail without dropped options or success placeholders; MCP helper certified separately if advertised |
| V33 | 2 route / SS-04, SS-10, SS-14 | Concurrent tasks, exception, task timeout, runner death, server outage, close/reattach/expiry | Independent sessions; truthful native commit/cancel state and eventual bounded cleanup; runner exit is not proof of compute cancellation; current MiniLake Jobs cancel is documented as between tasks unless upstream active cancellation exists |
| V34 | 4 optional / SS-13, SS-14 | Native path write → separate SDK registration → MiniLake/DuckDB read → native path read on shared local volume | Independently verified Delta rows/schema/decimal/time/commit versions; consistent actual files at /data; append/restart and registration-failure recovery; no atomic registration+commit or cloud-storage claim |
| V35 | 4 optional / SS-07, SS-13, SS-14 | Native named-table lookup through existing MiniLake UC API | Stable/drop-recreate IDs, exact ordered schema and snapshot consistency, per-session provider installation and bounded lookup; managed DuckDB/unsupported formats, misses, HTTP errors, stale metadata and denied paths fail without silent local fallback |
| V36 | 4 optional DML / SS-13, SS-14 | Agreed structured MiniLake SQL external-Delta write delegation | Pin actual upstream hook/native capability; independent reader verifies insert/update/delete for each enabled operation; operation identity, deadline, input lifetime, conflict/retry/unknown-commit outcomes and cleanup; no generated classic Spark/JVM fallback |

All evidence remains pending. Record MiniLake, runner, native binary, client/SDK, DuckDB/Delta dependencies, wheel hashes, image digests, exact commands, and enabled capability matrix. Register-only, TCP-only, Jobs-status-only, or DuckDB-only witnesses do not satisfy native route certification.

## 4.16 execution order

Follow the native delivery slices in [the 4.16 plan](RELEASE_4_16.md#ordered-delivery-slices). V05–V14 validate the direct HTTP/launcher surface and V29 checks the complete packaged candidate; these are the pre-publication requirements. V01–V04 certify optional upstream integrations and experimental Connect independently. V30 runs after publication and establishes release completion. Advertise external routes only after their separate certification passes.

The exact SQL golden corpus, proposed resource defaults, and negative cases are in the [4.16 implementation plan](RELEASE_4_16.md). Boundary tests should use smaller configured limits for determinism as well as confirming the selected production defaults; a request that times out must not release its live compute lease.

## 4.16 release publication gate

Only the native V05–V14 and V29 gates below are required before any 4.16.0 artifact is published. Change a status to `passed` only after recording its evidence. V05–V08 apply to direct HTTP values and schemas; SDK/emulator results are outside these publication gates.

| Gate | Status |
| --- | --- |
| V05 | passed |
| V06 | passed |
| V07 | passed |
| V08 | passed |
| V09 | passed |
| V10 | passed |
| V11 | passed |
| V12 | passed |
| V13 | passed |
| V14 | passed |
| V29 | passed |

### Recorded 4.16.0 candidate evidence

- Cross-platform release workflow [run 40009112934](https://github.com/eddiethedean/robin-sparkless/actions/runs/40009112934) passed on candidate commit `9f82adcb58f34cd3d0850bf521883233753cc07c`: 30 jobs succeeded, including the native server wheel matrix on Ubuntu 22.04 x86_64, macOS 15 arm64, macOS 15 Intel, and Windows. Each platform installed its wheel outside the checkout, ran `--version`, started the binary, checked `/health` and `SELECT 1`, then ran the direct HTTP/launcher suite. The same run passed all 15 Python compatibility jobs, Rust tests, format, clippy, audit, deny, and release metadata checks. The release-readiness job now also runs on `codex/release-*` candidate pushes.
- V05–V08: direct HTTP assertions passed for `SELECT 1 AS result`, exact BIGINT `9007199254740993`, high-precision DECIMAL precision/scale, typed null and empty schema, date/timestamp, ordered duplicate labels, and fail-closed malformed, unsupported, multiple-statement, and write requests. The local command `SPARKLESS_SERVER_BINARY="$PWD/target/debug/sparkless-server" PYTHONPATH="$PWD/python/sparkless" python3 -W error::ResourceWarning -m unittest discover -s scripts/server-tests -v` passed all 25 tests on Darwin arm64 (2026-10-23).
- V09: direct HTTP tests passed request-body, SQL, cell, whole-response, row, column, and session-cap rejection without truncation. `cargo test --locked -p sparkless-server --lib http::tests` passed the queue-deadline test with operation capacity saturated.
- V10: the same Rust unit target passed a controlled blocking native worker scenario: the response deadline elapsed and cancellation was signaled, the compute permit remained held while the worker was alive, `/health` stayed ready, and the permit drained only after the worker exited. Health handling does not acquire compute capacity.
- V11: the direct suite passed idempotent close and session reopen/isolation checks; `cargo test --locked -p sparkless-server sessions::tests --lib` passed the ownership and idle-reaper tests (recorded local preflight below).
- V12–V13: direct launcher tests passed sync and async normal/exception cleanup, async startup cancellation, sync startup timeout, startup port collision/crash, binary-version mismatch, restart with a new launch identity, nested distinct launchers, same-object re-entry rejection, and child reaping. Diagnostic tests passed split-chunk secret redaction and oversized-line truncation.
- V14: the four-platform workflow matrix above built and executed platform-native wheels and their SQL smoke tests. A local `linux/arm64` container build and run passed without Python or Java/JVM in the runtime image; `/health` returned ready and BIGINT value/schema were exact. Image digest: `sha256:94fa2972d79d12d9ad9e905075f535150d71f0d5001e145433fe20e91b6c19fe`.
- V29: clean consumer installation outside the checkout combined the candidate root and server wheels on Darwin arm64. Root wheel SHA-256: `bb200ee2ca7967bbc208e1c3a1d0a1c0b774a9adf18c8eb011d4eab95515c9f7`; server wheel SHA-256: `53d482bd59e4d6cb1ad30de00632af35e4056607279ed019b21f8338f29555cc`. `sparkless[server]` installed both exact 4.16.0 packages with no source checkout; the installed launcher resolved and ran the server, `/health` passed, and exact BIGINT/schema passed. The clean environment had neither gRPC nor PySpark importable. The server extra pins `sparkless-server-bin==4.16.0`; gRPC occurs only in the separate optional Connect test lock. Root Cargo patches are not used when consuming the prebuilt wheel.

These statuses certify the native 4.16 HTTP release scope only. They do not certify SDK/emulator, experimental Connect, or any optional MiniLake route. V30 remains a post-publication verification gate.

## Optional integration status

V01–V04 remain uncertified: the Sparkless Connect feasibility endpoint/harness is absent, the inspected emulator commit returns `result.text`, and no corrected immutable emulator image is recorded. These limitations restrict integration claims and do not block 4.16 publication. SDK model fixtures live in `scripts/server-integration-tests/` and run independently from the standard server acceptance suite.

## Local preflight evidence

The following checks ran in the local `Darwin arm64` worktree at source revision `375d83d8` on 2026-10-23. They check the current implementation and release machinery but do not substitute for any V05–V14 or V29 publication gate above. The worktree also contains uncommitted changes, so this revision identifies the base checkout rather than a release candidate.

| Check | Result / command |
| --- | --- |
| Rust workspace | Passed: `cargo test --locked --workspace --features sql,delta --lib --tests` (release workflow command) |
| Rust static checks | Passed: `cargo fmt --all -- --check`; `cargo clippy --locked --workspace --all-features --all-targets -- -D warnings` |
| Release metadata | Passed: `python3 -m unittest discover -s scripts -p 'test_*release*.py'` (15 tests, including optional upstream and mandatory native gate cases); `python3 scripts/check_release_versions.py --tag v4.16.0` |
| Server HTTP/launcher preflight | Passed: `SPARKLESS_SERVER_BINARY=target/debug/sparkless-server python3 -W error::ResourceWarning -m unittest discover -s scripts/server-tests -v` (20 direct HTTP/launcher tests; exact BIGINT/decimal/date/timestamp, null/empty schema, duplicate labels, unsupported/write failures, body/SQL/cell/response/session limits, session close/reopen, sync/async cleanup/restart/nesting, port collision, and diagnostic redaction). The optional SDK fixtures are now run separately from this release suite. |
| Session ownership unit checks | Passed: `cargo test --locked -p sparkless-server sessions::tests --lib` (close cancels registered work while keeping shared references alive; idle reaper retains referenced sessions and expires them after release) |
| Workflow syntax | Parsed: `.github/workflows/release.yml` and `.github/workflows/ci.yml` with Ruby `YAML.load_file` |
| Candidate release wheel | Passed locally: built with `MACOSX_DEPLOYMENT_TARGET=15.0`, installed `sparkless_server_bin-4.16.0-py3-none-macosx_15_0_arm64.whl`, and passed all 7 server/launcher acceptance tests against its executable |
| Other wheel platforms | Pending CI matrix; local arm64 macOS run does not validate Linux, x86_64 macOS, or Windows artifacts |
| Native container image | Passed locally on `linux/arm64`: `docker build --platform linux/arm64 --file crates/sparkless-server/Dockerfile --tag sparkless-server:4.16.0-preflight .`; image manifest digest `sha256:94fa2972d79d12d9ad9e905075f535150d71f0d5001e145433fe20e91b6c19fe`; runtime has no Python/Java, health returns ready, and HTTP SQL preserves BIGINT `9007199254740993` and `long` schema |
| Compatibility candidate environment | Passed install/import in `/tmp/sparkless-connect-4.0.4` on CPython 3.11/macOS arm64 from `scripts/server-tests/requirements-connect-4.0.4.lock`; imports `pyspark-client==4.0.4` and `databricks-sdk==0.156.0`. This does not exercise Spark Connect or the emulator result route. |
| Public SDK model fixtures | Passed: `/tmp/sparkless-connect-4.0.4/bin/python -m unittest discover -s scripts/server-integration-tests -p 'test_sdk_model_contract.py' -v` (2 tests; exact JSON_ARRAY BIGINT/DECIMAL strings, null, duplicate column labels, and typed empty schema). This does not exercise the emulator adapter. |
| Dependency audit and policy | Passed: `cargo audit` and `cargo deny check advisories bans sources`; `Cargo.lock` now resolves Tokio 1.54.1, ctutils 0.4.4, and chacha20 0.10.2 |
| Python type check | Passed: `cd python && mypy sparkless` |
| SDK/emulator and Connect traces | Blocked: candidate client/protocol/SDK pins exist, but Sparkless has no Connect feasibility endpoint and no corrected emulator image/owner-tracked fix is available; no integration trace recorded |

## Test implementation guidance

Use native unit/integration tests for schema mappings, admission ownership, state transitions, and error propagation. Add property/fuzz tests where malformed nested protocols or state-machine races justify them. Integration tests must use real engine computation, not programmed success responses.

Use separate client and server environments to demonstrate runtime independence. The launcher/SDK/Connect Python environment is allowed to contain Python; the native service image must not require it. Reference Spark may use Java in its own oracle environment, which does not invalidate a separate JVM-free native service/client witness. The MiniLake V31 deployment and its image-build recipe must contain no Java/JVM at any stage; never include that reference environment or classic-Spark fallback in the claimed MiniLake deployment.

Inspect actual public client outputs and transport traces. Do not patch SDK models, parse result.text as a substitute for normal SDK data, disable client reattachment, or accept schema-less success as conformance.

For concurrency/cancellation, use controlled blocking/safe-point scenarios to establish ownership semantics and real workloads to establish integration behavior. A response deadline is not proof that compute stopped. Benchmark memory includes intermediates and replay buffers.

## Release commands to add with implementation

Server source is present, but passing integration/release evidence is still required. Publication CI should use targeted format/clippy/tests for supported feature combinations, direct HTTP acceptance, and launcher/package tests on supported Python/platform versions. Run external routes and SS-14 MiniLake certification in separate optional lanes when those capabilities are evaluated. Record the actual commands and lockfile paths beside their evidence; do not provide fictional runnable commands or report these planned checks as passed.

## Evidence record template

- Gate ID / result (pending, passed, failed, or blocked):
- Owner / implementation issue or PR:
- Native commit and enabled features:
- Client/SDK/emulator/oracle versions and digests:
- Semantic settings and resource limits:
- Platform/hardware and exact command:
- Test output / logs / measured results:
- Limitations or blocker:

Upstream emulator, SDK, and experimental Connect failures affect only their optional route certification. They do not block the native HTTP release. Optional Phase 3/4 evidence is required only when shipping those capabilities.
