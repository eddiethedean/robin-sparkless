# SS-02: Rust crate, CLI, and native distribution

> **Status: implementation in place; platform and release evidence open.** Phase: server crate and CLI in 0A; launcher, binary wheel, and image foundations in 0B.
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

## Current implementation state

`crates/sparkless-server` contains the native HTTP service and `serve`/`control` commands, including a version-reporting command for installed binaries. The package inventory includes `sparkless-server` and `sparkless-server-bin`; the latter couples its executable version to `sparkless[server]`. The release workflow packages each publishable Rust crate and inspects its normalized archive manifest for matching versions, local dependency paths, or a leaked root patch table. It builds and installs the staged macOS arm64 and manylinux x86_64 wheels, audits the Linux wheel, and installs the candidate `sparkless[server]` wheel pair offline in a fresh consumer before launching SQL through the Python owner. It also gates publication on the pinned SDK integration and native container SQL/shutdown smoke. The container runs the native executable as a non-root user. Its 4.17 V32 job then installs the published Rust crate and Python server extra, reruns the pinned SDK mutation suite, and verifies the public container digest and SQL/shutdown behavior. A local macOS arm64 wheel and local native image have focused lifecycle witnesses.

Those implementations do not complete the acceptance criteria. Candidate CI still needs to pass on one integrated 4.17 revision; fresh external consumer installs, full public SDK scenarios, additional 4.16 prerequisites, and the V31/V32 artifact gates remain open. Keep the checkboxes above open until their exact platform and release evidence is recorded in [VALIDATION.md](VALIDATION.md).
