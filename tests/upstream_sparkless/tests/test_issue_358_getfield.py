"""Test issue #358: Column.getField for array index (PySpark API parity).

PySpark supports F.col("ArrayVal").getField(0) for array element access.
This test verifies getField(int) matches getItem(int) and getField(str) for struct.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue358GetField:
    """Test Column.getField for array and struct access."""

    def _get_unique_app_name(self, test_name: str) -> str:
        """Generate unique app name for parallel test execution."""
        import os
        import threading

        thread_id = threading.current_thread().ident
        process_id = os.getpid()
        return f"{test_name}_{process_id}_{thread_id}"

    def test_getfield_array_index(self):
        """Test getField(0) on array column (issue example)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "ArrayVal": ["E1", "E2"]},
                    {"Name": "Bob", "ArrayVal": ["E3", "E4"]},
                ]
            )
            df = df.withColumn("Extract-Field", F.col("ArrayVal").getField(0))
            rows = df.collect()
            assert len(rows) == 2
            assert rows[0]["Extract-Field"] == "E1"
            assert rows[1]["Extract-Field"] == "E3"
        finally:
            spark.stop()

    def test_getfield_equivalent_to_getitem(self):
        """Test getField(int) matches getItem(int)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame([{"arr": [10, 20, 30]}, {"arr": [40, 50]}])
            df_getfield = df.withColumn("x", F.col("arr").getField(1))
            df_getitem = df.withColumn("x", F.col("arr").getItem(1))
            rows_gf = df_getfield.collect()
            rows_gi = df_getitem.collect()
            assert [r["x"] for r in rows_gf] == [r["x"] for r in rows_gi]
            assert [r["x"] for r in rows_gf] == [20, 50]
        finally:
            spark.stop()

    def test_getfield_struct_field_by_name(self):
        """Test getField(str) on struct column."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            from sparkless.sql.types import (
                StructType,
                StructField,
                StringType,
                IntegerType,
            )

            schema = StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField(
                        "person",
                        StructType(
                            [
                                StructField("name", StringType(), True),
                                StructField("age", IntegerType(), True),
                            ]
                        ),
                        True,
                    ),
                ]
            )

            df = spark.createDataFrame(
                [(1, {"name": "Alice", "age": 30}), (2, {"name": "Bob", "age": 25})],
                schema=schema,
            )
            df = df.withColumn("person_name", F.col("person").getField("name"))
            rows = df.collect()
            assert rows[0]["person_name"] == "Alice"
            assert rows[1]["person_name"] == "Bob"
        finally:
            spark.stop()

    def test_getfield_nested_array_access(self):
        """Test getField on nested array structures."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"nested": [[1, 2], [3, 4], [5, 6]]},
                    {"nested": [[7, 8], [9, 10]]},
                ]
            )
            # Access first element of nested array
            df = df.withColumn("first_array", F.col("nested").getField(0))
            rows = df.collect()
            assert rows[0]["first_array"] == [1, 2]
            assert rows[1]["first_array"] == [7, 8]
        finally:
            spark.stop()

    def test_getfield_negative_index(self):
        """Test getField with negative index (last element).

        PySpark may return None for negative indices; Sparkless/Polars may
        support negative indexing. Accept either behavior.
        """
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [{"arr": [10, 20, 30]}, {"arr": [40, 50, 60, 70]}]
            )
            df = df.withColumn("last", F.col("arr").getField(-1))
            rows = df.collect()
            # PySpark returns None for negative index; Sparkless may return last element
            last0, last1 = rows[0]["last"], rows[1]["last"]
            if last0 is None and last1 is None:
                assert True  # PySpark behavior
            else:
                assert last0 == 30 and last1 == 70  # Negative-index behavior
        finally:
            spark.stop()

    def test_getfield_chained_access(self):
        """Test chained getField calls."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame([{"matrix": [[1, 2, 3], [4, 5, 6], [7, 8, 9]]}])
            # Access matrix[1][2] -> 6
            df = df.withColumn("val", F.col("matrix").getField(1).getField(2))
            rows = df.collect()
            assert rows[0]["val"] == 6
        finally:
            spark.stop()
