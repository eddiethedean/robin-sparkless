# Testing in repository and application CI

For a new application, begin with the [testing quickstart](TESTING_GUIDE.md#quick-start).
For repository work, use [contributor profiles](https://github.com/eddiethedean/robin-sparkless/blob/main/CONTRIBUTING.md#contributor-profiles).

## Maintainer checks and CI

For repository changes, follow [CONTRIBUTING.md](https://github.com/eddiethedean/robin-sparkless/blob/main/CONTRIBUTING.md#setup)
to create the uv environment and build the native extension. Run from the
repository root:

```bash
make check-full
uv run --no-project --python .venv/bin/python python -m pytest tests/ -v
make test-parity-phases
```

`make check-full` runs Rust checks/tests and Python lint/types; the separate
pytest command runs Python tests. Rebuild the native extension after Rust or
PyO3 changes before relying on local Python results.

The **CI** workflow builds one Linux wheel for its Python jobs. Normal runs test
ordinary cases, parity fixtures, and a separate serial Delta lane. Manual CI
runs also enable the full Python suite and PySpark smoke jobs. These are jobs in
CI rather than separate workflows. Release tests the publication wheel and
runs focused runtime smoke checks across supported wheel targets. See
[the release guide](RELEASING.md) for the current publication prerequisites.

## Expected skips

Skip counts vary with the selected backend, profile, dependencies, and markers.
Inspect each reason with `python -m pytest -rs`; do not treat a historical skip
count as a release target.

| Category | What to check |
| --- | --- |
| Delta | The serial lane uses `SPARKLESS_ENABLE_DELTA=1` and `-m delta` |
| Database/integration | Required database drivers, services, Docker, or fixtures are available |
| Backend/profile | `sparkless_only`, `pyspark_only`, `pyspark3_only`, and `pyspark4_only` match the selected run |
| Deferred functionality | The skip reason names the unsupported feature or environment requirement |

## CI Configuration

### GitHub Actions Example

This application workflow installs the 4.20.0 packages from PyPI. Its repository
needs a root conftest that registers `sparkless_testing.plugin`.

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test-sparkless:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: python -m pip install "sparkless[testing]==4.20.0"
      - run: pytest tests/ -v

  test-pyspark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - uses: actions/setup-java@v4
        with:
          distribution: "temurin"
          java-version: "17"
      - run: python -m pip install "sparkless[testing]==4.20.0" "pyspark==4.0.5"
      - run: SPARKLESS_TEST_MODE=pyspark SPARKLESS_PYSPARK_COMPAT=4.0 pytest tests/ -v
```

---
