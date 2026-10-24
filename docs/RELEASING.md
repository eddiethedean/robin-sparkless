# Releasing robin-sparkless

Use this guide with the [server release roadmap](plans/sparkless-server/ROADMAP.md), [4.16 implementation plan](plans/sparkless-server/RELEASE_4_16.md), and [4.17 implementation plan](plans/sparkless-server/RELEASE_4_17.md). A green source build is only one part of a release: the candidate workflow must verify the coordinated package inventory, public SDK integration, binary wheels, and native container before a release tag is ready.

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

4. Run the repository checks, including formatting, strict Clippy, Rust tests, Python lint/tests, and the pinned MiniLake public-SDK integration suite. Follow the phase plan for the exact V-gate scenarios; a focused component test does not substitute for the integrated gate.
5. Create or update `codex/release-X.Y.Z`, include the complete reviewed changes, and push it. The Release workflow performs non-publishing validation on `codex/release-*` branches. Review the full run, including packaged Rust crate archives, the candidate binary wheel builds, a fresh offline `sparkless[server]` install and query, and the native container SQL/graceful-shutdown smoke before tagging. Candidate jobs upload wheel and image artifacts for inspection but do not publish them.

The checker supports Python 3.11+ through `tomllib`; Python 3.8–3.10 developer environments need `tomli` installed.

## Required credentials and permissions

GitHub Actions needs `CARGO_REGISTRY_TOKEN` for crates.io and `PYPI_API_TOKEN` for PyPI. The server image publication job uses the workflow token with `packages: write` permission to publish to `ghcr.io/eddiethedean/sparkless-server`. Release tags are never overwritten; the container job also refuses to replace an existing image tag. Manual workflow dispatch defaults `publish_rust` to false and does not publish binary wheels, Python wheels, or a container.

## Tag and publication sequence

After the integrated candidate branch passes, merge it to `main`, verify the release commit is on `main`, then create and push the tag:

```bash
git switch main
git pull --ff-only origin main
git tag vX.Y.Z
git push origin vX.Y.Z
```

The tag workflow reruns every validation before the first publication. Its dependency order is:

1. Package the publishable Rust crates and verify the archives have matching versions, no local dependency paths, and no root patch table. Build and install-test `sparkless-server-bin` wheels on native macOS arm64 and manylinux x86_64 runners; run `auditwheel show` for the Linux wheel. Install the matching candidate `sparkless` and binary wheels from local artifacts, with package-index access disabled, then start the server through the public Python launcher and run an HTTP SQL query.
2. Build the `linux/amd64` native image. Verify its non-root runtime user, readiness, `SELECT 1 AS result` over HTTP, and graceful stop. Save the verified image as a workflow artifact. No package or image is published while these gates are running.
3. Publish Rust crates in dependency order: `spark-sql-parser`, `robin-sparkless-core`, `robin-sparkless-polars`, `robin-sparkless`, then `sparkless-server`.
4. Publish the tested `sparkless-server-bin` wheels to PyPI and the tested `vX.Y.Z` image to GHCR, refusing to overwrite an existing container tag.
5. Publish the `sparkless` Python extension wheels and source distribution across its configured Linux, macOS, and Windows matrix. These jobs wait for the Rust crates, server binary wheels, container, and adapter checks.
6. For 4.17 tags, run the V32 public-release job after every publication job succeeds. It installs the Rust server crate from crates.io and checks its version, verifies PyPI metadata and the required binary wheel tags, installs `sparkless[server]` from PyPI in a clean locked SDK environment, runs the pinned public-SDK mutation suite against that installed binary, and pulls, digest-checks, queries, and gracefully stops the GHCR image.

The workflow skips a crates.io version only when the registry reports it was already uploaded. PyPI wheel publication uses `--skip-existing`; this supports recovery from a partial registry run but does not make a partial release complete. Never edit or overwrite a published package. If published contents need correction, publish a new coordinated patch version.

## Verify the released artifacts

The 4.17 tag workflow runs V32 automatically after all publication jobs succeed. It verifies exact public package versions, installs the published Rust server crate, installs `sparkless[server]` in a fresh environment and runs the pinned MiniLake public-SDK mutation suite against the packaged binary, checks required PyPI server-wheel tags and file hashes, and pulls the GHCR tag to compare its immutable digest with the publish result before repeating HTTP SQL and shutdown checks. Review its evidence and record the final artifact URLs and hashes in the phase validation matrix. Add platform-specific public installs if the advertised server-extra matrix expands.

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
