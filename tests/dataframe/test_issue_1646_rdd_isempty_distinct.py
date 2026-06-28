"""Regression test for issue #1646: RDD isEmpty() and distinct()."""

from __future__ import annotations

from sparkless.testing import get_imports

StructField = get_imports().StructField
StructType = get_imports().StructType
StringType = get_imports().StringType


class TestIssue1646RddIsEmptyDistinct:
    """PySpark supports rdd.isEmpty() and rdd.distinct()."""

    def test_rdd_isempty_and_distinct_count(self, spark) -> None:
        """Exact scenario from issue #1646."""
        df = spark.createDataFrame([("A",), ("A",), ("B",)], ["col1"])
        assert df.rdd.isEmpty() is False
        assert df.rdd.distinct().count() == 2

    def test_rdd_isempty_true_on_empty_dataframe(self, spark) -> None:
        schema = StructType([StructField("col1", StringType())])
        df = spark.createDataFrame([], schema)
        assert df.rdd.isEmpty() is True

    def test_rdd_distinct_on_materialized_rdd(self, spark) -> None:
        df = spark.createDataFrame([(1,), (1,), (2,), (3,), (2,)], ["x"])
        rdd = df.rdd.flatMap(lambda row: (row["x"], row["x"] + 10))
        assert sorted(rdd.distinct().collect()) == sorted([1, 2, 3, 11, 12, 13])
