from sparkless import SparkSession
from sparkless.spark_types import StructType, StructField, StringType, LongType


class TestIssue202SelectWithList:
    """Test cases for issue #202: DataFrame.select() with list of column names."""

    def test_select_with_list_of_column_names(self):
        """
        Test that select() correctly handles a list of column names,
        matching PySpark's behavior.
        """
        spark = SparkSession("test_app")
        df = spark.createDataFrame(
            [
                {"name": "Alice", "dept": "IT", "salary": 50000},
                {"name": "Bob", "dept": "HR", "salary": 60000},
                {"name": "Charlie", "dept": "IT", "salary": 70000},
            ]
        )

        columns_to_select = ["name", "dept"]
        result = df.select(columns_to_select)

        # Verify schema
        expected_schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("dept", StringType(), True),
            ]
        )
        assert result.schema == expected_schema
        assert len(result.schema.fields) == 2

        # Verify data
        assert result.count() == 3
        rows = result.collect()
        assert len(rows) == 3
        assert rows[0].name == "Alice"
        assert rows[0].dept == "IT"
        assert rows[1].name == "Bob"
        assert rows[1].dept == "HR"
        assert rows[2].name == "Charlie"
        assert rows[2].dept == "IT"

    def test_select_with_tuple_of_column_names(self):
        """
        Test that select() also handles a tuple of column names.
        """
        spark = SparkSession("test_app")
        df = spark.createDataFrame(
            [
                {"name": "Alice", "dept": "IT", "salary": 50000},
                {"name": "Bob", "dept": "HR", "salary": 60000},
            ]
        )

        columns_to_select = ("name", "salary")
        result = df.select(columns_to_select)

        # Verify schema
        expected_schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("salary", LongType(), True),
            ]
        )
        assert result.schema == expected_schema
        assert len(result.schema.fields) == 2

        # Verify data
        assert result.count() == 2
        rows = result.collect()
        assert rows[0].name == "Alice"
        assert rows[0].salary == 50000
        assert rows[1].name == "Bob"
        assert rows[1].salary == 60000

    def test_select_with_single_column_list(self):
        """
        Test that select() handles a list with a single column name.
        """
        spark = SparkSession("test_app")
        df = spark.createDataFrame(
            [
                {"name": "Alice", "dept": "IT"},
                {"name": "Bob", "dept": "HR"},
            ]
        )

        result = df.select(["name"])

        # Verify schema
        expected_schema = StructType([StructField("name", StringType(), True)])
        assert result.schema == expected_schema
        assert len(result.schema.fields) == 1

        # Verify data
        assert result.count() == 2
        rows = result.collect()
        assert rows[0].name == "Alice"
        assert rows[1].name == "Bob"

    def test_select_with_multiple_args_still_works(self):
        """
        Ensure that the existing behavior of select() with multiple arguments
        is not regressed.
        """
        spark = SparkSession("test_app")
        df = spark.createDataFrame(
            [
                {"name": "Alice", "dept": "IT", "salary": 50000},
                {"name": "Bob", "dept": "HR", "salary": 60000},
            ]
        )

        # This should still work as before
        result = df.select("name", "dept")

        assert len(result.schema.fields) == 2
        assert result.count() == 2
        rows = result.collect()
        assert rows[0].name == "Alice"
        assert rows[0].dept == "IT"
        assert rows[1].name == "Bob"
        assert rows[1].dept == "HR"

    def test_select_star_with_list_does_not_unpack(self):
        """
        Test that select(["*"]) is not unpacked (should select all columns).
        """
        spark = SparkSession("test_app")
        df = spark.createDataFrame(
            [
                {"name": "Alice", "dept": "IT"},
                {"name": "Bob", "dept": "HR"},
            ]
        )

        # When list contains "*", it should be treated as selecting all columns
        result = df.select(["*"])

        assert len(result.schema.fields) == 2
        assert result.count() == 2
