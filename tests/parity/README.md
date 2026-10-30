# Pytest parity cases

These tests compare Sparkless with checked-in expected outputs, using
`tests.tools.parity_base.ParityTestBase`. Fixtures live in `tests/expected_outputs`;
inspect each fixture's oracle provenance. The loader's historical default is
PySpark 3.2, not a blanket Spark4 certification.

From the repository root after contributor setup:

```bash
.venv/bin/python -m pytest tests/parity --collect-only -q
.venv/bin/python -m pytest tests/parity/dataframe/test_select.py -v
.venv/bin/python -m pytest tests/parity -v
```

See [the current add-a-regression walkthrough](../../docs/TEST_CREATION_GUIDE.md)
for the real module import, fixture workflow, and dual-backend checks.
`tests/gen_pyspark_cases.py` writes the separate `tests/fixtures` corpus.
