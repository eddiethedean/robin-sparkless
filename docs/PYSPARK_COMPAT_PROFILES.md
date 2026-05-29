# PySpark compatibility profiles

Sparkless 4.9+ supports **compatibility profiles** that bundle Spark SQL and PySpark semantics. Profiles let you opt into **PySpark 4.x** behavior while keeping **PySpark 3.5** as the default.

See also: [PYSPARK_4_PARITY_PLAN.md](PYSPARK_4_PARITY_PLAN.md), [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md).

## Setting a profile

```python
from sparkless.sql import SparkSession

spark = SparkSession.builder.app_name("demo").get_or_create()

# Tier A (default): PySpark 3.5-like semantics
# spark.conf.set("sparkless.pyspark.compat", "3.5")  # optional; this is the default

# Tier B: PySpark 4.0+ semantics (ANSI on, map normalization, etc.)
spark.conf.set("sparkless.pyspark.compat", "4.0")
```

Environment (tests / CI):

```bash
export SPARKLESS_PYSPARK_COMPAT=4.0   # or 3.5 (default)
```

When `sparkless.pyspark.compat` is set, Sparkless applies the **bundle** below. Individual keys can still override the bundle after the profile is applied.

## Profile bundles

| Setting | `compat=3.5` (default) | `compat=4.0` |
|---------|------------------------|--------------|
| `spark.sql.ansi.enabled` | `false` | `true` |
| `spark.sql.legacy.disableMapKeyNormalization` | `true` | `false` |
| `spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled` | `true` | `false` |
| `spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled` | `true` | `false` |

## Individual overrides

After setting a profile, you may override specific keys (PySpark-compatible names):

| Key | Effect |
|-----|--------|
| `spark.sql.ansi.enabled` | When `true`, invalid casts, integer overflow, and divide-by-zero may raise errors instead of returning null. |
| `spark.sql.legacy.disableMapKeyNormalization` | When `true`, skip `-0.0` → `0.0` map key normalization. |
| `spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled` | When `true`, infer map schema from first non-null pair only (3.x style). |
| `spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled` | When `true`, infer array schema from first element only. |
| `spark.sql.legacy.postgres.datetimeMapping.enabled` | When `true`, PostgreSQL JDBC uses PySpark 3.5 datetime mapping. |
| `spark.sql.legacy.mysql.datetimeMapping.enabled` | When `true`, MySQL/MariaDB JDBC uses PySpark 3.5 type mapping. |
| `spark.sql.legacy.oracle.timestampMapping.enabled` | When `true`, Oracle JDBC uses PySpark 3.5 timestamp write mapping. |
| `spark.sql.legacy.mssqlserver.datetimeMapping.enabled` | When `true`, SQL Server JDBC uses PySpark 3.5 type mapping. |
| `spark.sql.legacy.db2.datetimeMapping.enabled` | When `true`, DB2 JDBC uses PySpark 3.5 type mapping. |

## Environment variables

| Variable | Values | Purpose |
|----------|--------|---------|
| `SPARKLESS_PYSPARK_COMPAT` | `3.5` (default), `4.0` | Applied at session creation if `sparkless.pyspark.compat` is not set |
| `PYSPARK_YM_INTERVAL_LEGACY` | `1` | Legacy `YearMonthIntervalType` shapes in `collect()` (4.0 PySpark change) |
| `SPARKLESS_TEST_MODE` | `sparkless`, `pyspark` | Dual-backend pytest ([TESTING_GUIDE.md](TESTING_GUIDE.md)) |

## Sparkless 5.0.0 note

Sparkless **5.0.0** will default to `compat=4.0`. **`compat=3.5` will remain available** via config for at least 12 months after 5.0.0 ships.
