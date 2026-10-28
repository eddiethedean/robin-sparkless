# SS-01: Compatibility contracts and typed results

> **Status: planned; owner unassigned.** Phase: 0A, with direct HTTP gate in 0B.
> [Index](README.md) · [Epic contract](EPIC.md#phase-0a-contracts-and-feasibility) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Own the version/semantics manifest and exact standalone HTTP/Connect client contracts. Sparkless development and releases use in-repository fixtures and standard clients, without requiring changes or acceptance in another application.

## Dependencies and delivery slices

The contract inventory has no implementation prerequisite; real HTTP validation needs SS-05. Later Connect conformance belongs to SS-09/10.

## Implementation sequence

1. Inventory one exact pyspark-client release in the Spark 4.0 family and its matching proto tag/commit, Python range, gRPC/protobuf/PyArrow dependencies, and reference-server version. Record selections, lockfile locations, and evidence in DECISIONS.md.
2. Specify SQL semantics independently of wire versions: ANSI, timezone, case sensitivity, decimals, and allowed configuration. Keep launcher, HTTP envelope, package, protocol, and engine versions distinct.
3. Freeze typed positional HTTP rows and logical schemas. Preserve exact integers and decimal scale without intermediate floating-point conversion.
4. Build decoder fixtures for ordered columns, duplicate labels, empty typed results, nulls, BIGINT `9007199254740993`, and high-precision decimals. Verify actual HTTP output and packaged consumers once SS-05 is ready.
5. Treat optional external adapters/proxies as supplemental interoperability. Before advertising one, record its versions and independent value/schema/lifecycle evidence. External fixes, maintainer acceptance, and merge timing do not gate standalone Sparkless releases.

## Acceptance criteria

- [ ] An immutable compatibility manifest and typed-result fixtures exist; no invented pins or claimed support.
- [ ] Native/direct HTTP and packaged consumers preserve exact values and schemas in V02/V05–V08.
- [ ] Standalone release tests run without checking out another application's repository.

## Decisions and implementation touchpoints

Resolve D01–D04 in the [decision register](DECISIONS.md) and record actual evidence in [VALIDATION.md](VALIDATION.md).

- [python/pyproject.toml](../../../python/pyproject.toml)
- [Runtime configuration](../../../crates/robin-sparkless-core/src/runtime_config.rs)
