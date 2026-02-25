# Vendored upstream sparkless tests

This directory contains the test suite from the [eddiethedean/sparkless](https://github.com/eddiethedean/sparkless) repository (main branch), vendored so we can run it against this repo’s PyO3-backed `sparkless` package and track parity.

## Layout

- **`tests/`** – Upstream test tree: `unit/`, `parity/`, `integration/`, `fixtures/`, `expected_outputs/`, `tools/`, etc. The top-level `conftest.py` in this directory wires imports and the `spark` fixture to our package.
- **`xfail_list.txt`** – Central list of test path fragments that are marked as expected failure (known unsupported APIs / parity gaps). Burn this down as we add support.
- **`pytest.ini`** – Pytest markers and options for this subtree.

## Running tests

To run tests against **this repo’s** sparkless package (robin-sparkless PyO3 build), install it and ensure no other `sparkless` is active:

```bash
pip uninstall sparkless -y   # remove upstream if present
cd python && maturin develop # install this repo’s sparkless
```

From the **repo root**:

```bash
# Fast subset (excludes delta and integration)
make test-python-upstream

# Or explicitly:
pytest tests/upstream_sparkless -m "not delta and not integration" -v --tb=short

# Full suite (includes delta, integration; best-effort)
make test-python-upstream-full
pytest tests/upstream_sparkless -v --tb=short
```

Run a single file or directory:

```bash
pytest tests/upstream_sparkless/tests/unit/dataframe/ -v
pytest tests/upstream_sparkless/tests/test_issue_160_with_cache_enabled.py -v
```

## Updating from upstream

1. Clone the upstream repo and copy the contents of upstream `tests/` into `tests/upstream_sparkless/tests/` (so that `unit/`, `fixtures/`, etc. live under that directory):

   ```bash
   git clone --depth 1 https://github.com/eddiethedean/sparkless.git /tmp/sparkless-upstream
   cp -R /tmp/sparkless-upstream/tests/* tests/upstream_sparkless/tests/
   ```

2. Do **not** overwrite:
   - `tests/upstream_sparkless/conftest.py` (our shims and xfail hook)
   - `tests/upstream_sparkless/xfail_list.txt`
   - `tests/upstream_sparkless/README.md`
   - `tests/upstream_sparkless/pytest.ini`

3. Re-run the suite and adjust `xfail_list.txt` for any new failures due to unsupported APIs.

## xfail list

Edit `xfail_list.txt` to add or remove test path fragments. Any test whose node id contains one of these patterns is marked `xfail(strict=False)`. Use this for:

- Tests that depend on internal APIs we don’t expose (e.g. `sparkless.backend.factory`)
- Known parity gaps we’re tracking

Remove entries as we implement support.

## Collection errors

Some vendored tests import upstream-internal modules (e.g. `sparkless.session.sql.optimizer`, `sparkless.spark_types`) that this package does not expose. Those modules fail at collection time. We apply xfail only to tests that *collect* but are known to fail. To reduce collection errors over time you can add shims for more internal modules in `conftest.py` or skip specific files via a collection filter. CI runs the upstream suite with `continue-on-error: true` so collection errors do not fail the job.
