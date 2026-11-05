# Security considerations and reporting

Sparkless executes native code, local file IO, and optionally Python callbacks
and database requests with the caller's privileges. Treat it as trusted-process
code, not a sandbox. The native server has no built-in TLS/authentication or
hard per-query CPU/intermediate-memory isolation. Use the [deployment boundary](PRODUCTION.md).

## Known dependency exception

For **4.18.0**, ordinary unpatched crates.io consumer graphs retain the temporary
zstd advisory exception expiring **2026-11-26**. Official wheels/server/container/
repository builds use the fixed implementation. This distinction also applies
to source distributions as described in the [versioned notice](security/zstd-4.18-exception.md).
Rust consumers should apply its exact immutable Cargo patch; the exception is
risk acceptance, not remediation. The notice records scope, approval, evidence,
and expiry. Review it before installing an unpatched Rust dependency.

The [4.19.0 source lockfile](https://github.com/eddiethedean/robin-sparkless/blob/v4.19.0/Cargo.lock)
resolves `zstd` 0.14.3 and `zstd-safe` 8.2.0. A Rust application resolves its own
dependency graph. Review its `Cargo.lock` and audit results separately.
See [release evidence](RELEASE_EVIDENCE.md) for per-file hashes and available
PyPI provenance. For the DuckDB server, the installed package also includes
the Quack extension and its notice.

## Reporting

Start at the repository's [Security page](https://github.com/eddiethedean/robin-sparkless/security).
If GitHub offers **Report a vulnerability**, use that private channel for sensitive
reports. If no private reporting option is available, open a minimal issue asking
maintainers for a private contact, without exploit details, credentials or affected
private data. Do not assume a private channel exists merely because this page does.

Include affected versions/features, reproduction conditions, impact, and possible
mitigation through the agreed channel. There is no guaranteed response time or
backport window; see [support expectations](SUPPORT.md). Current dependency audit
results and exceptions are release evidence, not a blanket security certification.
