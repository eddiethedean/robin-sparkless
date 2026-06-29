# Production Deployment

Summary of optional **security hardening** and deployment notes for Sparkless. Sparkless targets **local testing and development**, not replacement of a managed Spark cluster. See [Before you adopt](BEFORE_YOU_ADOPT.md).

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `SPARKLESS_HARDENED` | off | Enable stricter defaults when set to `1` |
| `SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL` | true | Set to `false` to disallow arbitrary JDBC SQL; use `dbtable` only |
| `SPARKLESS_FILES_BASE` | unset | When set, confine file read/write paths to this directory |

Details and additional keys: [PySpark differences — Security hardening](PYSPARK_DIFFERENCES.md#security-hardening-optional).

## JDBC

- Prefer **`dbtable`** over free-form SQL when `SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL=false`.
- Use read-only database credentials where possible.
- Connection strings and secrets should come from your secret manager, not source code.

## File IO

Set `SPARKLESS_FILES_BASE` to a sandbox directory in CI or shared runners to prevent path traversal to sensitive paths.

## CI recommendations

```yaml
# Example: GitHub Actions
- run: pip install "sparkless>=4,<5"
- run: pytest tests/unit -v
  env:
    SPARKLESS_HARDENED: "1"
```

- Cache pip wheels for faster installs.
- Pin Sparkless version range per release branch.
- Use dual-mode testing selectively (PySpark job is slower; run nightly or on main).

See [examples/python/](https://github.com/eddiethedean/robin-sparkless/tree/main/examples/python) and [Testing guide](TESTING_GUIDE.md).

## What not to expect

- No distributed fault tolerance, executor isolation, or Spark ACL model.
- No guarantee of bit-identical results with cluster PySpark for all operations.

For parity gaps: [Deferred scope](DEFERRED_SCOPE.md), [Parity status](PARITY_STATUS.md).
