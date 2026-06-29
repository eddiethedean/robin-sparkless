"""Example: dual-mode pytest with sparkless.testing.

Run sparkless backend (default):
    pytest examples/python/test_dual_mode_example.py -v

Run against real PySpark (requires Java + sparkless[pyspark]):
    SPARKLESS_TEST_MODE=pyspark pytest examples/python/test_dual_mode_example.py -v
"""

import pytest
from sparkless.sql import SparkSession, functions as F


@pytest.fixture
def spark():
    session = SparkSession.builder.app_name("dual_mode_example").get_or_create()
    yield session
    session.stop()


def test_filter_and_aggregate(spark):
    df = spark.createDataFrame(
        [
            {"team": "A", "score": 10},
            {"team": "A", "score": 20},
            {"team": "B", "score": 5},
        ]
    )
    totals = df.groupBy("team").agg(F.sum("score").alias("total"))
    rows = {r["team"]: r["total"] for r in totals.collect()}
    assert rows == {"A": 30, "B": 5}
