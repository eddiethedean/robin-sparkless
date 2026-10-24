# SS-11: Emulator gRPC integration, benchmarks, and release

> **Status: release evidence in progress.** Phase: 0B release foundations; Phase 2 proxy and concurrent-client release.
> [Index](README.md) · [Epic contract](EPIC.md#acceptance-and-release-gates) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Provide reproducible release evidence for each supported route and measure performance without unsupported claims.

For [4.17](RELEASE_4_17.md), 417-07/08 require prior applicable regressions plus V15–V18, V31 isolated candidate installs, and V32 fresh public-artifact verification. Catalog/mutation tests cover both direct HTTP and the corrected pinned SDK/emulator route; native schema/laziness evidence and bounded retention claims need measured results. Republish the complete inventory at 4.17.0 while production gRPC remains unsupported.

## Dependencies and delivery slices

HTTP release needs SS-01–08 applicable 0B slices; gRPC proxy certification requires standalone SS-09/10 conformance.

Plan numbers are workstream identifiers, not a strict execution order. Only the applicable phase slice blocks a milestone; later extensions must not delay an earlier gate.

## Implementation sequence

1. Create separate CI gates for direct HTTP, SDK through emulator, standalone Connect, and Connect through emulator. Failure of an external route must not be hidden by a standalone success.

2. Pin emulator and client revisions/digests. Verify proxy h2c transport, cluster/session handles, auth boundary, metadata, streaming, cancellation, and reconnect.

3. Run native images with no Python/JVM; run launchers/SDKs/clients in separate environments. Record evidence that the Rust engine computed results.

4. Test feature-isolated crate builds, format/clippy, packaging, supported OS/architecture lifecycle, and protocol decoder/state-machine fuzz/property coverage.

5. Benchmark correctness, cold start, throughput, p50/p95, RSS, CPU, and batch behavior at 1/2/4/8+ clients using fixed datasets/settings and appropriate Spark/Sail baselines.

6. Publish raw commands, hardware/software versions, warmup, concurrency limits, results, unsupported surfaces, and release artifacts. Separate I/O responsiveness from CPU parallelism.

## Acceptance criteria

- [ ] Every advertised route has its own passing immutable test evidence in VALIDATION.md.
- [ ] Proxy tests exercise actual authentication/session handling and reconnect rather than just TCP reachability.
- [ ] Benchmarks validate outputs and report resource/latency tradeoffs; no unmeasured speedup or zero-copy claim.
- [ ] Published docs match enabled capabilities, persistence, cooperative limits, and platform matrix.

## Decisions and implementation touchpoints

Resolve or explicitly defer D01, D03, D05, D08 in the [decision register](DECISIONS.md). Record evidence and immutable revisions in the [validation matrix](VALIDATION.md); writing this plan does not complete a gate.

- [.github/workflows](https://github.com/eddiethedean/robin-sparkless/tree/main/.github/workflows)
- [docs/RELEASING.md](../../RELEASING.md)
- [docs/INTEGRATION_TEST_GUIDE.md](../../INTEGRATION_TEST_GUIDE.md)

## Scope notes

DATABRICKS_SPARK_CONNECT_GRPC_URL configures the proxy backend (HTTP/h2c); clients still use the appropriate Spark Connect connection contract.

The release workflow now includes the pinned MiniLake public-SDK suite, Rust archives checked for matching versions and absence of local dependency paths/root patches, candidate server-binary wheel builds and fresh-install lifecycle checks, an offline `sparkless[server]` install through the public launcher, manylinux audit, and a non-root native container HTTP-SQL/graceful-shutdown gate. Tag publication waits for those checks, then publishes Rust crates, server wheels and image, and the Python extension in sequence. For 4.17 tags, V32 installs the public Rust/Python packages, reruns SDK mutation tests against the packaged binary, and verifies the public container digest and behavior. A local macOS arm64 wheel and native container have been built for focused verification; these are not integrated-candidate or public-artifact evidence. Keep V15–V18, V31, V32, and the full adapter matrix open until their required revisions and outcomes are recorded in [VALIDATION.md](VALIDATION.md).
