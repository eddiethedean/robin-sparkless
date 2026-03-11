"""
Regression test for issue #1385: errors.missing_column parity.

PySpark scenario (from the issue):

    def scenario_errors_missing_column(session):
        if _backend_is_pyspark(session):
            from pyspark.sql import functions as F  # type: ignore
        else:
            from sparkless.sql import functions as F  # type: ignore

        df = session.createDataFrame([(1,)], ["x"])
        return df.select(F.col("nope").alias("y"))

Both backends should error when selecting a non-existent column; this test
locks in Sparkless' error type and message shape.
"""

import pytest

from sparkless.sql import SparkSession, functions as F
from sparkless.errors import SparklessError


def test_issue_1385_errors_missing_column_message() -> None:
    """errors.missing_column: selecting a non-existent column should raise SparklessError (issue #1385)."""
    spark = SparkSession.builder.appName("issue_1385").getOrCreate()
    try:
        df = spark.createDataFrame([(1,)], ["x"])

        with pytest.raises(SparklessError) as excinfo:
            _ = df.select(F.col("nope").alias("y")).collect()

        msg = str(excinfo.value)
        # Lock in the existing unresolved-column message shape so future changes
        # remain intentional and visible in tests.
        assert "unresolved_column: cannot be resolved: not found" in msg
        assert "cannot resolve: column 'nope' not found" in msg
        assert "Available columns: [x]" in msg
    finally:
        spark.stop()

