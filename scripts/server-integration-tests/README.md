# Optional upstream integration fixtures

These SDK model fixtures are independent of native HTTP release readiness.
They do not certify the emulator adapter or a Spark Connect endpoint.

Run from the repository root in an isolated Python 3.11 environment:

```sh
python -m pip install -r scripts/server-tests/requirements-connect-4.0.4.lock
python -m unittest discover -s scripts/server-integration-tests -v
```

The standard server acceptance suite in `scripts/server-tests/` requires no
Databricks SDK or PySpark client installation. Publication requires native
V05–V14 and V29 evidence in `docs/plans/sparkless-server/VALIDATION.md`.
