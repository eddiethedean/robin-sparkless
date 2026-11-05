# sparkless-testing

Backend-independent pytest fixtures and helpers for test suites that run against
Sparkless or PySpark. Install `sparkless[testing]` when testing a Sparkless
application, or install `sparkless-testing` directly for a suite that selects its
backend independently.

Add this to your suite's root `conftest.py` to load the fixtures and backend
markers:

```python
pytest_plugins = ["sparkless_testing.plugin"]
```

The package does not import Sparkless, PySpark, a native engine, or a JVM until a
backend-dependent helper is called. Follow the
[testing guide](https://robin-sparkless.readthedocs.io/en/latest/TESTING_GUIDE/)
to install an engine and run the same test on both backends.
