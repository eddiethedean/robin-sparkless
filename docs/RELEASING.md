# Releasing robin-sparkless

Use this guide with the [server release roadmap](plans/sparkless-server/ROADMAP.md), [4.16 implementation plan](plans/sparkless-server/RELEASE_4_16.md), [4.17 implementation plan](plans/sparkless-server/RELEASE_4_17.md), and [4.18 implementation plan](plans/sparkless-server/RELEASE_4_18.md). A green source build is only one part of a release: the candidate workflow must verify the coordinated package inventory, public SDK integration, binary wheels, and native container before a release tag is ready.

For 4.18, the Release workflow runs standalone Spark Connect conformance against the pinned PySpark client and Spark 4.0.5 reference, measures the pinned parent and candidate sequentially on one hosted Linux runner for V26, and verifies the exact candidate distributions and server transports. V25 emulator certification is required only if that external route is advertised; it is not a prerequisite for the standalone 4.18 Connect release. Review the candidate run and keep tagging/publishing blocked until its required gates pass.

The roadmap assigns additional Connect API/types, I/O/artifacts/cache, durable recovery, and Structured Streaming to 4.23–4.26 (SS-16–19 / V39–V47), and scalar/Pandas UDFs to Phase 5 V27. These are planned release requirements, not implemented workflow jobs. Add feature-specific candidate/public-artifact and optional worker/connector package checks before each release; preserve the default Python/JVM-free server and prior capability regressions.

## Release inventory and version policy

For coordinated releases beginning with **4.16.0**, all first-party packages use the exact same stable semantic version:

- Rust crates: `spark-sql-parser`, `robin-sparkless-core`, `robin-sparkless-polars`, `robin-sparkless`, and `sparkless-server`.
- Python packages: `sparkless` and `sparkless-server-bin`; the `sparkless[server]` extra pins the matching binary package exactly.
- Rust lockfile entries, internal dependency requirements, `sparkless.__version__`, and the release tag (`vX.Y.Z`) must agree.

Before the coordinated 4.16 line, `spark-sql-parser` may have its legacy independent version. For 4.16+ the parser is part of the same release graph; the checker rejects stale parser manifest, dependency, or lockfile versions. Run the checker against candidate branches without a tag as well as against the final tag.

The release workflow currently stages server binary wheels for `macosx_14_0_arm64` and `manylinux_2_28_x86_64`, plus a `linux/amd64` native container. These are the only supported server-extra and server-container targets until additional platform artifacts pass the same install and lifecycle checks. The ordinary `sparkless` Python extension has its separate existing platform matrix.

## Before creating a candidate

1. Bump the first-party manifests and internal dependency versions to `X.Y.Z`. Update `Cargo.lock`, `python/sparkless/sparkless/__init__.py`, `python/pyproject.toml`, and `python/server-bin/pyproject.toml` together. Do not unintentionally upgrade third-party dependencies during a patch release.
2. Add a dated `[X.Y.Z] - YYYY-MM-DD` section to `CHANGELOG.md` and update install examples or capability documentation affected by the release.
3. Run the metadata checker and its tests:

   ```bash
   python3 -m unittest discover -s scripts -p 'test_*release*.py'
   python3 scripts/check_release_versions.py
   python3 scripts/check_release_versions.py --tag vX.Y.Z
   ```

4. Run the repository checks, including formatting, strict Clippy, Rust tests, Python lint/tests, and the in-repository native server unit, control-protocol, and live HTTP integration suites (`cargo test --locked --release -p sparkless-server`). External MiniLake tests are not part of CI or the release checks. Follow the phase plan for the exact V-gate scenarios; a focused component test does not substitute for the integrated gate.
5. Create or update `codex/release-X.Y.Z`, include the complete reviewed changes, and push it. The Release workflow performs non-publishing validation on `codex/release-*` branches. Review the full run, including packaged Rust crate archives, all eight `sparkless` platform wheels plus its sdist, both supported server-binary wheels, the exact offline `sparkless[server]` install and query, and the native container SQL/graceful-shutdown smoke. V31 checks package names, versions, metadata, and platform tags, then preserves those exact verified distributions as workflow artifacts. Candidate jobs do not publish them.

Release 4.18 requires Python 3.11+ for both Python distributions. All extension wheels use `cp311-abi3`; candidate metadata checks reject older ABI tags or a lower `Requires-Python` value. Python 3.8–3.10 are no longer supported or shipped. The historical 4.16 recovery workflow retains its original interpreter pins.

## Required credentials and permissions

GitHub Actions needs `CARGO_REGISTRY_TOKEN` for crates.io. PyPI publishing uses Trusted Publishing (OIDC), so no `PYPI_API_TOKEN` secret is required. In **each** existing PyPI project (`sparkless` and `sparkless-server-bin`), add a GitHub Actions publisher with owner `eddiethedean`, repository `robin-sparkless`, workflow filename `release.yml`, and environment `pypi`. These are two project-level registrations with the same publisher identity. The historical `recover-4.16.0.yml` workflow has a separate identity, but it cannot repair the immutable 4.16.0 files and is not used for future releases. A corrective 4.16.1 and coordinated 4.17.0/4.18.0 releases publish through `release.yml`. The server image publication job uses the workflow token with `packages: write` permission to publish to `ghcr.io/eddiethedean/sparkless-server`. Release tags are never overwritten; the container job also refuses to replace an existing image tag. Manual workflow dispatch defaults `publish_rust` to false and does not publish binary wheels, Python wheels, or a container.

## Tag and publication sequence

After the integrated candidate branch passes, use the release-line policy below before creating and pushing the tag.

- For the next planned release line, merge the candidate to `main`, verify the release commit is on `main`, then tag that commit.
- For a corrective patch to an already published line (for example, 4.16.1 correcting 4.16.0), keep the candidate based on that release line and tag the verified candidate commit directly. Do not merge newer development into the patch tag; selectively backport maintenance-only workflow or verifier fixes to `main` separately.

This distinction keeps maintenance package contents aligned with the published release while allowing `main` to continue the next release line.

For a next-line release, the sequence is:

```bash
git switch main
git pull --ff-only origin main
git tag vX.Y.Z
git push origin vX.Y.Z
```

The tag workflow reruns every validation before the first publication. Its dependency order is:

1. Package the publishable Rust crates and verify the archives have matching versions, no local dependency paths, and no root patch table. Build and install-test `sparkless-server-bin` wheels on native macOS arm64 and manylinux x86_64 runners; run `auditwheel show` for the Linux wheel. Build the complete `sparkless` matrix and sdist, then validate the exact distribution inventory. Install the matching candidate `sparkless` and binary wheels from local artifacts, with package-index access disabled, then start the server through the public Python launcher and verify SQL, exact values/schema, mutations, replay, and retained status. Audit a separately resolved Rust consumer graph with all root patches removed; an unaccepted consumer finding blocks publication. For 4.18.0 only, the [documented zstd exception](security/zstd-4.18-exception.md) accepts three exact registry tuples through 2026-11-26 UTC while preserving the raw findings and decision report. Official artifact graphs and the supplied consumer patch configuration must still be fixed; the latter audits strictly without exceptions.
2. Build the `linux/amd64` native image. Verify its non-root runtime user, readiness, SQL, shared-catalog mutations/replay/status, and graceful stop. Save the verified image as a workflow artifact. No package or image is published while these gates are running.
3. Publish Rust crates in dependency order: `spark-sql-parser`, `robin-sparkless-core`, `robin-sparkless-polars`, `robin-sparkless`, then `sparkless-server`. This job waits for V31.
4. Publish the tested `sparkless-server-bin` wheels to PyPI with OIDC and the tested `vX.Y.Z` image to GHCR, refusing to overwrite an existing container tag.
5. Publish the exact verified `sparkless` Python extension wheels and source distribution to PyPI with OIDC, after the Rust crates, server binary wheels, and image are published.
6. For 4.18 tags, run V32 after every publication job succeeds. It installs the Rust server crate from crates.io under the disclosed 4.18.0 consumer exception and functionally exercises that exact executable, verifies every PyPI filename and SHA-256 against the V31 distributions, installs `sparkless[server]` in a clean consumer environment and runs standalone HTTP catalog, typed-value, identity/replay, and retained-status checks, then pulls, digest-checks, queries, and gracefully stops the GHCR image.

The workflow skips a crates.io version only when the registry reports it was already uploaded. The PyPI publishing action skips existing files to allow a run to resume after a partial upload; V32 still requires the public set and digests to match the verified candidate exactly. Never edit or overwrite a published package. If published contents need correction, publish a new coordinated patch version.

## Verify the released artifacts

The 4.18 tag workflow runs V32 automatically after all publication jobs succeed. It verifies exact public package versions and file hashes against V31, installs the published Rust server crate, installs `sparkless[server]` in a fresh environment and runs `scripts/verify_server_extra_install.py X.Y.Z --mutations` against the packaged binary, and pulls the GHCR tag to compare its immutable digest with the publish result before repeating HTTP SQL and shutdown checks. External MiniLake tests are not part of these checks. Review its evidence and record the final artifact URLs and hashes in the phase validation matrix. Add platform-specific public installs if the advertised server-extra matrix expands.

Record artifact URLs, wheel tags and hashes, image digest, source commit, tool versions, commands, and observed results in the phase validation matrix. A local wheel or image smoke is useful development evidence, but it does not pass the candidate inventory gate or replace a fresh public-artifact check.

## Manual Rust publication

Manual publication is an exception for maintainers handling a clearly scoped Rust-only recovery. Preserve the same version inventory and dependency order:

```bash
cargo publish --locked -p spark-sql-parser --token <CRATES_IO_TOKEN>
cargo publish --locked -p robin-sparkless-core --token <CRATES_IO_TOKEN>
cargo publish --locked -p robin-sparkless-polars --token <CRATES_IO_TOKEN>
cargo publish --locked -p robin-sparkless --token <CRATES_IO_TOKEN>
cargo publish --locked -p sparkless-server --token <CRATES_IO_TOKEN>
```

Manual crate publication does not publish or verify the Python packages or native image and does not complete the release gates.

Build the Python source distribution through the project PEP 517 backend (`python -m build --sdist --outdir dist python`). Direct `maturin sdist` omits root Cargo patch directories. Run `python scripts/verify_release_source.py dist/sparkless-X.Y.Z.tar.gz` to check an isolated locked source build and the fixed compression dependency graph before accepting it for V31.
