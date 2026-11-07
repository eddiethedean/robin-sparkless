# Historical Docker compatibility matrix

These scripts describe the older Python 3.11–3.13 and PySpark 3.4/3.5
matrix. The Docker template expects the old `mock_spark` source tree and
test paths, so it cannot build from a Sparkless 4.20.0 checkout.

For 4.20.0, use the [testing guide](../../docs/TESTING_GUIDE.md#quick-start)
to run the same pytest suite against Sparkless and the documented PySpark
3.5.8 or 4.0.5 profiles. See the [Python compatibility profiles](../../docs/PYSPARK_COMPAT_PROFILES.md)
and [installation matrix](../../docs/INSTALLATION.md#platforms-and-build-features)
for the supported combinations and wheel targets.
