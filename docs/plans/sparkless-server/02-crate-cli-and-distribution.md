# SS-02: Rust crate, CLI, and native distribution

> **Status: source implemented; platform and release evidence pending.** Phase: Minimal scaffolding in 0A; supported CLI/image in 0B.
> [Index](README.md) · [Epic contract](EPIC.md#workspace-build-and-distribution) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Build a separately installable Rust service without adding transport dependencies to embedded engine consumers.

## Dependencies and delivery slices

SS-01 defines compatibility; SS-04 supplies startup/shutdown; SS-05 supplies HTTP readiness.

Plan numbers are workstream identifiers, not a strict execution order. Only the applicable phase slice blocks a milestone; later extensions must not delay an earlier gate.

## Implementation sequence

1. Add crates/sparkless-server as a workspace library/executable with direct versioned path dependency on robin-sparkless and sql. Follow edition, MSRV, and pinned toolchain conventions.

2. Separate HTTP and Spark Connect features. Start with only the scaffolding needed for SS-03; do not wait for a production package to investigate the protocol.

3. Implement serve CLI/config precedence, loopback defaults, explicit remote bind, explicit/ephemeral ports, bounded configuration values, capabilities, and startup errors.

4. Specify the owned readiness/control channel with SS-06. Emit readiness only after native startup checks and listener binding; support graceful drain from the CLI and launcher.

5. Build a minimal runtime image and native artifacts. Inspect runtime dependencies and feature graphs to verify no Python/JVM dependency.

6. Publish a tested OS/architecture/libc matrix, install/build commands, artifact checksums, and release ordering. Validate package archives and independent cargo installation before release.

For 4.16, follow [416-02 and 416-08](RELEASE_4_16.md): inventory every first-party package, update parser dependency requirements with its version transition, and prove candidate consumer installs do not inherit the root workspace's Cargo patches or local source paths. Version validation and publishing must include the server and binary Python distribution.

## Acceptance criteria

- [ ] Crate builds with targeted features; HTTP/gRPC dependencies do not enter the embedded core's normal dependency graph.
- [ ] CLI binds configured/ephemeral ports, detects collisions, reports actual endpoints, and drains on shutdown.
- [ ] Released images execute real SQL without Python or a JVM; each advertised platform passes build and lifecycle checks.

## Decisions and implementation touchpoints

Resolve or explicitly defer D05, D06 in the [decision register](DECISIONS.md). Record evidence and immutable revisions in the [validation matrix](VALIDATION.md); writing this plan does not complete a gate.

- [Cargo.toml](https://github.com/eddiethedean/robin-sparkless/blob/main/Cargo.toml)
- [rust-toolchain.toml](https://github.com/eddiethedean/robin-sparkless/blob/main/rust-toolchain.toml)
- [docs/RELEASING.md](../../RELEASING.md)

## Scope notes

The crate, CLI, feature-gated HTTP surface, control protocol, binary-wheel workflow, native image, and GHCR publication job are implemented in the source tree. Clean consumer installs and platform-specific release evidence remain pending.
