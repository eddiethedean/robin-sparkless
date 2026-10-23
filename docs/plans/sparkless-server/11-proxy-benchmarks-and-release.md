# SS-11: Emulator gRPC integration, benchmarks, and release

> **Status: planned; owner unassigned.** Phase: 0B release foundations; Phase 2 proxy and concurrent-client release.
> [Index](README.md) · [Epic contract](EPIC.md#acceptance-and-release-gates) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Provide reproducible release evidence for each supported route and measure performance without unsupported claims.

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
