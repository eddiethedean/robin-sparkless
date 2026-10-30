# Installation and versions

Documentation contract: **4.18.0 candidate**. At the 2026-10-29 review, public
PyPI and crates.io both served **4.17.0**. Installing an unversioned package does
not install this candidate. Check `python -m pip show sparkless` before following
a version-specific guide. This snapshot stays historical after publication;
[PyPI files](https://pypi.org/project/sparkless/#files) are the artifact authority.

## Published stable package

```bash
python3.11 -m venv .venv
# POSIX; Windows: .venv\Scripts\activate
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install "sparkless==4.17.0"
python -c "import sparkless; print(sparkless.__version__)"
```

Use the [4.17 source documentation](https://github.com/eddiethedean/robin-sparkless/tree/v4.17.0/docs)
for stable behavior. `/en/latest/` follows development and can describe unreleased
features. After publication, install the exact desired version (`==4.18.0`) and
use its tagged source docs. Local Python needs no Java or Rust when a wheel matches.

## Candidate artifacts from GitHub CI

Install GitHub CLI and authenticate for artifact downloads. Pick a successful
**Release** validation run on the exact candidate commit, inspect its artifacts,
and download only your platform's wheel archive:

```bash
gh run list --repo eddiethedean/robin-sparkless --workflow release.yml --limit 5
gh run view RUN_ID --repo eddiethedean/robin-sparkless
gh api repos/eddiethedean/robin-sparkless/actions/runs/RUN_ID/artifacts --jq '.artifacts[].name'
gh run download RUN_ID --repo eddiethedean/robin-sparkless --name ARTIFACT_NAME --dir candidate
python -m pip install --no-index --find-links candidate "sparkless==4.18.0"
```

Replace `RUN_ID` and `ARTIFACT_NAME` with values from that run. Install the matching
`sparkless-server-bin==4.18.0` artifact too before using server launchers; the
`server` extra cannot resolve an unpublished dependency from PyPI. Artifacts expire;
if absent, use the [canonical contributor build](https://github.com/eddiethedean/robin-sparkless/blob/main/CONTRIBUTING.md#setup)
on the exact commit. A successful validation run does not imply publication.

## Platforms and build features

Extension wheels use **cp311-abi3**, compatible with supported CPython 3.11–3.14,
not only CPython 3.11. Wheels cover Linux glibc 2.17+ on x86_64, glibc 2.28+ on ARM64,
and musl 1.2+ on both architectures,
macOS Intel/Apple Silicon, and Windows x86_64/ARM64. Use wheel filenames for exact
macOS deployment floors (the ARM64 extension starts at macOS 11; the separate
server starts at macOS 14). Server binaries cover glibc 2.28+ Linux x86_64 and
macOS 14+ ARM64 only. Other targets require a source build.

| Distribution | SQL | Delta | Database drivers |
| --- | --- | --- | --- |
| Official `sparkless` wheel/default source build | Enabled | Enabled | JDBC/SQLite not enabled by default |
| Root Rust crate default features | Disabled | Disabled | Disabled |
| Rust crate with selected Cargo features | `sql` | `delta` | `jdbc` (Postgres), `sqlite`, `jdbc_mysql`, `jdbc_mariadb`, `jdbc_mssql`, `jdbc_oracle`, `jdbc_db2` |
| Packaged server | HTTP allowlist | No public Delta contract | No general database connector contract |

Python extras install Python dependencies, not native feature switches:
`dev` adds test tools; `jdbc` adds integration tools/drivers; `pyspark` adds Spark
3.5/Delta3 (Java11+); `pyspark4` adds Spark4/Delta4 (Java17+); `server` adds the
exact matching binary package. Keep the two PySpark extras in separate environments.

After canonical source setup, a SQLite/Postgres-enabled development extension:

```bash
(cd python && ../.venv/bin/python -m maturin develop --uv --locked --release --features sqlite,jdbc)
```

This retains default Delta and mandatory SQL. A database extra alone cannot
change a downloaded wheel. See [JDBC tests](JDBC_TESTING.md) for external services.
Rust consumers must read the [4.18 security notice](security/zstd-4.18-exception.md):
the unpatched Cargo graph has a time-bounded zstd exception through **2026-11-26**;
apply the pinned consumer patch. Repository/official distributions use the fixed graph.
