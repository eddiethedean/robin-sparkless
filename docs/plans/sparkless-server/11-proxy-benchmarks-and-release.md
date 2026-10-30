---
search:
  exclude: true
---

> **Archive / historical record:** this page describes a past snapshot or a plan.
> Use the [current support contract](../../SUPPORT_MATRIX.md) for release behavior.

# SS-11: Standalone conformance, benchmarks, and release

> **Status: release evidence in progress.** Phase: 0B release foundations; Phase 2 Connect and concurrent-client release.
> [Index](README.md) · [Epic contract](EPIC.md#acceptance-and-release-gates) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Provide reproducible release evidence for each supported route and measure performance without unsupported claims.

For [4.17](RELEASE_4_17.md), 417-07/08 require prior applicable regressions plus V15–V18, V31 isolated candidate installs, and V32 fresh public-artifact verification. Required catalog/mutation tests cover the complete native/direct-HTTP matrix; native schema/laziness evidence and bounded retention claims need measured results. Republish the complete inventory at 4.17.0 while production gRPC remains unsupported.

The later [DuckDB source phase](14-duckdb-data-source.md) and [DuckDB execution phase](15-duckdb-connect-backend.md) have separate V35–V38 gates after supported Connect. Apply these release/install/benchmark practices to each optional feature, measuring the source/service and execution backend together. Shared DuckDB access must be verified with independent clients per ownership mode; Connect connectivity alone does not prove it.

The roadmap now assigns expanded API/types/analysis, I/O/artifacts/cache, durable recovery, and Structured Streaming to SS-16–19 / 4.23–4.26 with V39–V47; Phase 5 V27 also includes scalar/Pandas UDFs. These are planned gates, not existing CI coverage. Each release must add its complete candidate and fresh-public-artifact jobs, preserve prior regressions, and certify optional worker/JDBC/storage packages independently before advertising them.

For [4.18](RELEASE_4_18.md), 418-06 established standalone Connect before optional 418-08 proxy certification and concurrent-client benchmarks. The candidate workflow passed V17, the integrated V18–V23 client/oracle corpus, packaged V24 checks, the approved V26 budget, and V33 candidate inventory; see the [cut evidence](RELEASE_4_18.md#4180-cut-evidence). V26 acceptance uses the reviewed `tests/connect/v26-regression-budget.json`, which records the approver, baseline run/revision and hardware/workload, per-cohort baseline throughput/p95/RSS, and numeric limits. The budget was approved and passed for this candidate; the earlier planning note that it was missing is superseded. The V25 route harness remains opt-in because the separate MiniLake Connect route is unavailable and is not advertised in 4.18. V34 fresh public-artifact verification remains after publication. Existing HTTP/SDK/4.17 jobs do not certify gRPC. Keep protocol source research separate from actual interoperability evidence.

## Dependencies and delivery slices

HTTP release needs SS-01–08 applicable 0B slices; Connect release requires standalone SS-09/10 conformance. Optional proxy certification is supplemental and nonblocking.

Plan numbers are workstream identifiers, not a strict execution order. Only the applicable phase slice blocks a milestone; later extensions must not delay an earlier gate.

## Implementation sequence

1. Require in-repository native unit/control/live-HTTP tests and fresh packaged-consumer checks for HTTP releases; add standalone unmodified-client conformance for supported Connect.

2. Certify any optionally advertised external proxy/adapter separately after standalone conformance, including values/schema, authentication, sessions, deadlines, cancellation, and reconnect. No external repository, contribution, or maintainer acceptance is a standalone release prerequisite.

3. Run native images with no Python/JVM; run launchers/SDKs/clients in separate environments. Record evidence that the Rust engine computed results.

4. Test feature-isolated crate builds, format/clippy, packaging, supported OS/architecture lifecycle, and protocol decoder/state-machine fuzz/property coverage.

5. Benchmark correctness, cold start, throughput, p50/p95, RSS, CPU, and batch behavior at 1/2/4/8+ clients using fixed datasets/settings and appropriate Spark/Sail baselines.

6. Publish raw commands, hardware/software versions, warmup, concurrency limits, results, unsupported surfaces, and release artifacts. Separate I/O responsiveness from CPU parallelism.

## Acceptance criteria

- [ ] Every advertised route has its own passing immutable test evidence in VALIDATION.md.
- [ ] Any advertised proxy route exercises authentication/session handling and reconnect rather than just TCP reachability.
- [ ] Benchmarks validate outputs and report resource/latency tradeoffs; no unmeasured speedup or zero-copy claim.
- [ ] Published docs match enabled capabilities, persistence, cooperative limits, and platform matrix.

## Decisions and implementation touchpoints

Resolve or explicitly defer D01, D03, D05, D08 in the [decision register](DECISIONS.md). Record evidence and immutable revisions in the [validation matrix](VALIDATION.md); writing this plan does not complete a gate.

- [.github/workflows](https://github.com/eddiethedean/robin-sparkless/tree/main/.github/workflows)
- [docs/RELEASING.md](../../RELEASING.md)
- [docs/INTEGRATION_TEST_GUIDE.md](../../INTEGRATION_TEST_GUIDE.md)

## Scope notes

Optional proxy configuration is owned by that integration; standalone clients use the supported Spark Connect connection contract.

For Slice 418-08, run `tests/connect/run_slice8_benchmarks.py` with the pinned standalone client to collect startup, correctness, latency, throughput, memory, CPU, health/HTTP progress, slow-reader, reconnect, and graceful-shutdown evidence. It records measurements without asserting a speedup or inventing numeric regression budgets. Run `tests/connect/run_slice8_proxy_route.py` separately with `SPARKLESS_PROXY_URL`, the proxy's immutable source revision, and a secret token to test an available route. The report marks cluster target selection and externally owned proxy shutdown as uncovered unless the integration supplies its own routing witness and lifecycle controls. See [Slice 8 instructions](https://github.com/eddiethedean/robin-sparkless/blob/main/tests/connect/README.md#slice-8-benchmark-and-proxy-route).

The release workflow now includes the in-repository native server integration suite, Rust archives checked for matching versions and absence of local dependency paths/root patches, candidate server-binary wheel builds and fresh-install lifecycle checks, an offline `sparkless[server]` install through the public launcher, manylinux audit, and a non-root native container HTTP-SQL/graceful-shutdown gate. Tag publication waits for those checks, then publishes Rust crates, server wheels and image, and the Python extension in sequence. For 4.17 tags, V32 installs the public Rust/Python packages, runs standalone HTTP catalog, typed-value, replay, and retained-status checks against the packaged binary, and verifies the public container digest and behavior. A local macOS arm64 wheel and native container have been built for focused verification; these are not integrated-candidate or public-artifact evidence. Keep V15–V18, V31, V32, and the required native/direct-HTTP matrices open until their required revisions and outcomes are recorded in [VALIDATION.md](VALIDATION.md).
