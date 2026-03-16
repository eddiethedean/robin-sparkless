"""
Regression test for issue #1385: errors.missing_column parity.

Both backends must error when selecting a non-existent column. Same scenario
and logic in sparkless and PySpark mode (use spark + spark_imports).
"""

from __future__ import annotations

import pytest



def test_issue_1385_errors_missing_column_message(spark, spark_imports) -> None:
    """Selecting a non-existent column raises an error (issue #1385)."""
    F = spark_imports.F
    df = spark.createDataFrame([(1,)], ["x"])

    with pytest.raises(Exception) as excinfo:
        _ = df.select(F.col("nope").alias("y")).collect()

    msg = str(excinfo.value).lower()
    assert (
        "nope" in msg
        or "not found" in msg
        or "cannot resolve" in msg
        or "unresolved" in msg
    )
