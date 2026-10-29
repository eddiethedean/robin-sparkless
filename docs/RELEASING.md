# Releasing robin-sparkless

Normal CI owns formatting, Clippy, dependency audits, Rust feature tests, server
HTTP/Connect regression tests, Python typing, ordinary/parity tests, and the serial
Delta suite. Release builds the distributions, checks the actual packages, and
publishes them. Publication requires a successful **CI** workflow run for the exact
release commit, from a push or manual dispatch. Pull-request runs validate a merge
commit and do not satisfy this publication check.

## Prepare the release

All first-party packages share one stable `X.Y.Z` version:

- Rust: `spark-sql-parser`, `robin-sparkless-core`, `robin-sparkless-polars`,
  `robin-sparkless`, and `sparkless-server`.
- Python: `sparkless` and `sparkless-server-bin`. The `sparkless[server]` extra pins
  the matching server package exactly.

Update manifests, internal dependency requirements, `Cargo.lock`,
`python/sparkless/sparkless/__init__.py`, and the dated changelog entry together.
Then check metadata and workflow helpers (Python 3.11+, with PyYAML installed):

```bash
python3 -m pip install pyyaml
python3 scripts/check_release_versions.py
python3 scripts/check_release_versions.py --tag vX.Y.Z
python3 -m unittest discover -s scripts -p 'test_*release*.py'
```

Push `codex/release-X.Y.Z` for a non-publishing candidate run. Manual **Release**
dispatches also build and validate the same artifacts without publishing by default.
There is one validation path for branches, tags, and manual runs.

For a new release line, merge the reviewed candidate to `main`, wait for CI on the
merge commit, and tag that commit:

```bash
git switch main
git pull --ff-only origin main
git tag vX.Y.Z
git push origin vX.Y.Z
```

For a maintenance release, tag the tested maintenance commit directly. First run
**CI** manually on that branch and wait for success on the exact commit being tagged.
If a tag was pushed before CI completed, rerun its failed release jobs after CI passes.

## What release checks

The release workflow retains all eight `cp311-abi3` extension wheel targets:
Linux glibc and musl on x86_64/ARM64, macOS Intel/ARM64, and Windows x64/ARM64.
It builds one Python sdist and checks the publishable Rust package archives.

The broad Python suite runs once against the Linux publication wheel. Focused
native smoke tests run SQL, nested/nullable values, Parquet, and Delta on **every**
wheel with Python 3.11 and 3.14. Linux checks use the declared glibc baseline or
musl runtime. Both supported server wheels (`manylinux_2_28_x86_64` and
`macosx_14_0_arm64`) run HTTP/Connect lifecycle checks and fresh offline
`sparkless[server]` installs.

The Linux server is compiled once. The release container copies the executable
from that tested wheel, then checks its non-root user, HTTP mutations, public
Connect client, and graceful shutdown. The source-build Dockerfile remains
available for local development.

One acceptance job requires every validation job to succeed, checks package
names, versions, ABI/platform inventory and Twine metadata, and stages the exact
checked distributions. Failed, cancelled, or skipped required jobs block all
publishers. There is no separate job inventory manifest or evidence aggregation
layer.

## Publication and retries

Stable tag pushes publish Rust crates in dependency order, then the server wheels
and container in parallel, then `sparkless`. The final job checks public PyPI file
names/hashes and crates.io versions. Publishers download the checked artifacts;
they do not rebuild wheels or containers. Rust publication uses `--no-verify`
because the same commit has already passed CI and package-archive checks.

For interrupted publication, use **Re-run failed jobs** on the original tagged run
while its artifacts are retained. Crates.io skips only an already-uploaded version;
PyPI skips existing files and the public inventory check verifies their hashes.
The container publisher accepts an existing tag only when its image config digest
matches the tested candidate. It refuses to overwrite different contents. The
container artifact is retained for seven days; distribution artifacts use GitHub's
default retention. Publish corrected contents under a new coordinated patch version.

Manual **Release** dispatch with `publish_rust: true` remains available on `main`
only, with the same validation and CI prerequisite. It publishes only Rust crates.
The former cross-run `pypi_recovery_run_id` input has been retired in favor of
rerunning the original run. `recover-4.16.0.yml` is historical and is not the recovery
path for current releases.

## Optional diagnostics

Run **Release diagnostics** manually on the commit you want to investigate. It
contains the paired V26 concurrency benchmark, Spark 4.0.5 oracle conformance,
range/catalog/HTTP memory measurements, downstream consumer audits, and isolated
sdist-to-wheel build. These checks retain their reports and failure behavior, but
do not delay routine candidates or publication. The release plans under
`docs/plans/sparkless-server/` describe the historical V-gate scenarios; this guide
and the workflows define the current release process.

## Credentials

Rust publication needs `CARGO_REGISTRY_TOKEN`. Both PyPI projects use Trusted
Publishing with owner `eddiethedean`, repository `robin-sparkless`, workflow
`release.yml`, and environment `pypi`. GHCR publication uses the workflow token's
`packages: write` permission for `ghcr.io/eddiethedean/sparkless-server`.
