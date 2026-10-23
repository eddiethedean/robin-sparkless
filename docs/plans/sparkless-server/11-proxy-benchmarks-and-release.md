# SS-11: Emulator routes, MiniLake certification, benchmarks, and release

> **Status: planned; owner unassigned.** Phase: 0B release foundations; Phase 2 proxy and concurrent-client release.
> [Index](README.md) · [Epic contract](EPIC.md#acceptance-and-release-gates) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Provide reproducible release evidence for each supported route and measure performance without unsupported claims.

## Dependencies and delivery slices

HTTP publication requires native V05–V14 and V29: native contracts, packaging, runtime, HTTP, launcher, sessions, and scalar schema. SS-01 external certification, SS-03 Connect feasibility, SDK/emulator work, and gRPC proxy certification do not block it. Certifying a gRPC proxy route requires standalone SS-09/10 conformance. [SS-14](14-minilake-interoperability.md) supplies a separate MiniLake Jobs route in Phase 2 and optional shared Delta/catalog routes in Phase 4. Neither substitutes for an existing route or adds a 4.16 gate.

Plan numbers are workstream identifiers, not a strict execution order. Only the applicable phase slice blocks a milestone; later extensions must not delay an earlier gate.

## Implementation sequence

1. Gate HTTP publication on direct native HTTP, launcher, and packaged-artifact CI. Run SDK through `calvinchengx/databricks-emulator`, standalone experimental Connect, emulator-proxied Connect, and MiniLake Jobs as separate optional certification lanes. MiniLake shared Delta/catalog and optional native DML lanes are separately feature-gated. Failure of any advertised external route must not be hidden by a standalone or DuckDB-only success.

2. Pin emulator and client revisions/digests. Verify proxy h2c transport, cluster/session handles, auth boundary, metadata, streaming, cancellation, and reconnect.

3. Run native images with no Python/JVM; run launchers/SDKs/clients in separate environments. Record evidence that the Rust engine computed results. For MiniLake additionally inspect its Python service, task runner, build recipe, spawned commands, and image pulls: no Java/JRE/JVM, Apache Spark distribution, JAR warming, or classic fallback anywhere in that deployment. Keep reference Spark outside this witness.

4. Test feature-isolated crate builds, format/clippy, packaging, supported OS/architecture lifecycle, and protocol decoder/state-machine fuzz/property coverage.

5. Benchmark correctness, cold start, throughput, p50/p95, RSS, CPU, and batch behavior at 1/2/4/8+ clients using fixed datasets/settings and appropriate Spark/Sail baselines.

6. Publish raw commands, hardware/software versions, warmup, concurrency limits, results, unsupported surfaces, and release artifacts. Separate I/O responsiveness from CPU parallelism.

## Acceptance criteria

- [ ] Every advertised route has its own passing immutable test evidence in VALIDATION.md.
- [ ] Proxy tests exercise actual authentication/session handling and reconnect rather than just TCP reachability.
- [ ] Benchmarks validate outputs and report resource/latency tradeoffs; no unmeasured speedup or zero-copy claim.
- [ ] Published docs match enabled capabilities, persistence, cooperative limits, and platform matrix.
- [ ] Advertised MiniLake Jobs pass V31–V33; optional shared Delta/catalogs pass V34–V35; SQL external-Delta DML requires V36. Pin each wheel/image/client/native revision and label all unsupported paths.

## Decisions and implementation touchpoints

Resolve or explicitly defer D01, D03, D05, D08 in the [decision register](DECISIONS.md). Record evidence and immutable revisions in the [validation matrix](VALIDATION.md); writing this plan does not complete a gate.

- [.github/workflows](https://github.com/eddiethedean/robin-sparkless/tree/main/.github/workflows)
- [docs/RELEASING.md](../../RELEASING.md)
- [docs/INTEGRATION_TEST_GUIDE.md](../../INTEGRATION_TEST_GUIDE.md)

## Scope notes

DATABRICKS_SPARK_CONNECT_GRPC_URL configures the proxy backend (HTTP/h2c); clients still use the appropriate Spark Connect connection contract.

The `DATABRICKS_*` agent/proxy settings do not apply to MiniLake. Its existing Jobs image/environment seams reach Connect directly; its SQL warehouse remains DuckDB. Document its between-task Jobs cancellation limit separately from native Connect interruption and runner timeouts. Optional upstream proposals are tracked only; creating issues or messaging maintainers requires separate user authorization.
