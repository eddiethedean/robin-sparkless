# sparkless-testing

Backend-independent pytest fixtures and helpers for test suites that run against
Sparkless or PySpark. Install `sparkless[testing]` when testing a Sparkless
application, or install `sparkless-testing` directly for a suite that selects its
backend independently.

```python
pytest_plugins = ["sparkless_testing.plugin"]

from sparkless_testing import get_imports
```

The package does not import Sparkless, PySpark, a native engine, or a JVM until a
backend-dependent helper is called.
