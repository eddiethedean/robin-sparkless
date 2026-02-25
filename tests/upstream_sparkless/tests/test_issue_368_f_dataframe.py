"""Test issue #368: F.DataFrame for reduce(F.DataFrame.union, dfs).

PySpark allows F.DataFrame.union so that reduce(F.DataFrame.union, dfs) works.
This test verifies sparkless.sql.functions exposes DataFrame for API parity.
"""

from functools import reduce

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue368FDataFrame:
    """Test F.DataFrame for union_all pattern."""

    def _get_unique_app_name(self, test_name: str) -> str:
        """Generate unique app name for parallel test execution."""
        import os
        import threading

        thread_id = threading.current_thread().ident
        process_id = os.getpid()
        return f"{test_name}_{process_id}_{thread_id}"

    def test_f_dataframe_union_reduce(self):
        """Test reduce(F.DataFrame.union, dfs) as in PySpark (issue example)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice"},
                    {"Name": "Bob"},
                ]
            )
            df2 = spark.createDataFrame(
                [
                    {"Name": "Charlie"},
                    {"Name": "Disco"},
                ]
            )

            def union_all(*dfs):
                return reduce(F.DataFrame.union, dfs)

            df = union_all(df1, df2)
            rows = df.collect()
            names = [r["Name"] for r in rows]
            assert names == ["Alice", "Bob", "Charlie", "Disco"]
        finally:
            spark.stop()

    def test_f_has_dataframe_attribute(self):
        """Test that F.DataFrame is available (module __getattr__)."""
        assert hasattr(F, "DataFrame")
        from sparkless.sql import DataFrame as DataFrameClass

        assert F.DataFrame is DataFrameClass

    def test_f_dataframe_union_multiple_dfs(self):
        """Test reduce(F.DataFrame.union, dfs) with 4+ DataFrames."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df1 = spark.createDataFrame([{"id": 1, "val": "a"}])
            df2 = spark.createDataFrame([{"id": 2, "val": "b"}])
            df3 = spark.createDataFrame([{"id": 3, "val": "c"}])
            df4 = spark.createDataFrame([{"id": 4, "val": "d"}])

            result = reduce(F.DataFrame.union, [df1, df2, df3, df4])
            rows = result.collect()
            assert len(rows) == 4
            ids = sorted([r["id"] for r in rows])
            assert ids == [1, 2, 3, 4]
        finally:
            spark.stop()

    def test_f_dataframe_union_empty_df(self):
        """Test union with empty DataFrames."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df1 = spark.createDataFrame([{"x": 1}, {"x": 2}])
            df2 = spark.createDataFrame([], schema="x: int")

            result = reduce(F.DataFrame.union, [df1, df2])
            rows = result.collect()
            assert len(rows) == 2
            assert [r["x"] for r in rows] == [1, 2]
        finally:
            spark.stop()

    def test_f_dataframe_unionbyname(self):
        """Test F.DataFrame.unionByName is also accessible."""
        assert hasattr(F.DataFrame, "unionByName")
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df1 = spark.createDataFrame([{"a": 1, "b": 2}])
            df2 = spark.createDataFrame([{"b": 3, "a": 4}])  # Different column order

            result = F.DataFrame.unionByName(df1, df2)
            rows = result.collect()
            assert len(rows) == 2
            assert rows[0]["a"] == 1
            assert rows[1]["a"] == 4
        finally:
            spark.stop()
