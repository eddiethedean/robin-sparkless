# Sparkless 4.18.0 compression patch and temporary Rust consumer exception

On 2026-10-29 the repository release approver authorized a time-bounded security
exception for **4.18.0 only, expiring after 2026-11-26 (UTC)**. This accepts residual risk in
ordinary crates.io Rust builds; it does not make the vulnerable versions safe.
The policy is [machine-readable](../../security/zstd-4.18-exceptions.toml).
Tracking: [release PR #1727](https://github.com/eddiethedean/robin-sparkless/pull/1727).

## Which distributions are fixed

The repository's four vendored dependency patches require `zstd 0.14.3`,
`zstd-safe 8.2.0` and `zstd-sys 2.1.2+zstd.1.5.7`. Official Python wheels,
`sparkless-server-bin` wheels, native workspace builds, containers, and the
Python source archive use this fixed graph. The release gate checks the entire
workspace graph independently of the exception, and the source archive verifier
checks its graph and compiles it in isolation.

Cargo does not inherit a library dependency's workspace patches. Installing our
Rust crates or using plain `cargo install sparkless-server` therefore still
resolves `zstd 0.13.3` / `zstd-safe 7.3.0` through the upstream registry crates.
This includes the ordinary crates.io installation exercised by V32. Rust users
handling untrusted compressed input should apply the consumer patch below or use
the prebuilt server distribution.

The current standalone HTTP allowlist exposes no file readers. That limits HTTP
reachability of the Parquet issue; embedded Rust APIs can read Parquet files.

## Accepted findings and remaining exposure

| Advisory | Exact registry package | Scope and rationale |
| --- | --- | --- |
| [RUSTSEC-2026-0391](https://github.com/RustSec/advisory-db/blob/main/crates/zstd/RUSTSEC-2026-0391.md) | `zstd 0.13.3` | **Real residual exposure in unpatched Rust consumers.** Polars Parquet reuses a decompression context. A partial/malformed page can leave a stream unfinished and disclose bytes from an earlier page when the context is reused. Accepting this advisory is risk acceptance, not an unused-API assertion. Our patch routes that path through the upstream session-reset fix in `zstd 0.14.3`. |
| [RUSTSEC-2026-0403](https://github.com/RustSec/advisory-db/blob/main/crates/zstd-safe/RUSTSEC-2026-0403.md) | `zstd-safe 7.3.0` | The advisory concerns uninitialized output after failed legacy-frame streaming decompression through the **write decoder**. Current Polars/Arrow/Parquet call sites use read decoders or bulk APIs, which the advisory excludes. Other downstream application code may use the affected API. Our patch replaces the implementation with fixed `8.2.0`. |
| [RUSTSEC-2026-0398](https://rustsec.org/advisories/RUSTSEC-2026-0398.html) | `zstd-safe 7.3.0` | The affected seekable API is disabled in the all-features Sparkless consumer graph. The gate rejects that feature on this version. Another downstream dependency can enable it through Cargo feature unification; those users must reassess their own graph. Our patch replaces the implementation with fixed `8.2.0`. |

The existing abandoned encoder, abandoned decoder, and partial-Parquet-page
regressions in `crates/robin-sparkless-polars/tests/zstd_context_regression.rs`
exercise the session-isolation fix. The earlier vulnerable-code reproducer is
recorded in [historical evidence](../history/zstd-013-prototype-evidence.md).

## Apply our patch in a Rust consumer

Download [security/zstd-4.17-patches.toml](../../security/zstd-4.17-patches.toml)
from this release. It points Cargo at the four patched packages in immutable
commit `49d92c1f7c33f3b66f0d6b0ec1f7314f757b6f8c`. These package manifests use the
fixed upstream zstd implementations; this is an actual code/dependency fix.

Merge its `[patch.crates-io]` table into **your application's root `Cargo.toml`**
or `.cargo/config.toml`, preserving any existing configuration. Build once to
update your lockfile, then commit the patch configuration and lockfile. Do not
depend on our crate's configuration propagating to your project.

For a one-command test, Cargo also accepts the file directly:

```bash
cargo --config /absolute/path/to/zstd-4.17-patches.toml build
cargo --config /absolute/path/to/zstd-4.17-patches.toml build --locked
cargo audit
```

For a secure crates.io server installation:

```bash
cargo --config /absolute/path/to/zstd-4.17-patches.toml install --version '=4.18.0' sparkless-server
```

The install resolves the patched graph rather than reusing the unpatched
published lockfile. Keep this configuration for future rebuilds. It requires
GitHub access. Inspect your resolved graph for any additional zstd versions
introduced by other application dependencies; this file targets the dependency
versions used by Sparkless 4.18.0.

The release workflow verifies this exact public Git configuration in an isolated
consumer with `--strict`; no exception is permitted on that patched graph.

## Gate behavior and removal

There are no new global `cargo-audit` or `cargo-deny` ignores. Only the independent
Rust consumer gate reads this policy, after asserting that the shipped workspace
uses the fixed versions. It retains the unmodified Cargo audit JSON, including
`vulnerabilities.found = true`, alongside an `*.exceptions.json` decision report.
It accepts only the three advisory/package/version tuples above from crates.io.
Different versions, new non-ignored vulnerabilities, tool errors, a changed release number,
activation of `seekable` on `zstd-safe 7.3.0`, or expiry fail the gate.

Use `scripts/verify_rust_consumer_graph.py --strict --output report.json` to
observe the unresolved ordinary consumer failures. Use `--patch-config
security/zstd-4.17-patches.toml --strict` to verify the remediated route. The existing
repository advisory policy for unrelated dependencies is unchanged. All other
release gates still have to pass. This exception alone does not certify a release.

Remove the exception and consumer patch once upstream dependency versions resolve
only fixed implementations and the strict consumer audit and behavioral tests
pass. [Polars PR #30316](https://github.com/pola-rs/polars/pull/30316) tracks the
upstream update. A subsequent Sparkless version or a release after the expiry
requires a fresh decision if it still contains the affected dependency graph.
