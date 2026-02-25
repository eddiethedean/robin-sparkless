from sparkless import SparkSession
from sparkless.spark_types import StringType, LongType


class TestIssue203FilterWithString:
    """Test cases for issue #203: DataFrame.filter() with string expressions."""

    def test_filter_with_string_expression(self):
        """
        Test that filter() correctly handles string SQL expressions,
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

        # Test filter with string expression
        result = df.filter("salary > 55000")

        # Verify schema is preserved (field order may vary, so check fields individually)
        assert len(result.schema.fields) == 3
        field_names = {field.name for field in result.schema.fields}
        assert field_names == {"name", "dept", "salary"}
        # Check field types
        field_types = {field.name: field.dataType for field in result.schema.fields}
        assert field_types["name"] == StringType()
        assert field_types["dept"] == StringType()
        assert field_types["salary"] == LongType()

        # Verify data
        assert result.count() == 2
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0].name == "Bob"
        assert rows[0].salary == 60000
        assert rows[1].name == "Charlie"
        assert rows[1].salary == 70000

    def test_filter_with_string_equals(self):
        """
        Test filter with string equality expression.
        """
        spark = SparkSession("test_app")
        df = spark.createDataFrame(
            [
                {"name": "Alice", "dept": "IT", "salary": 50000},
                {"name": "Bob", "dept": "HR", "salary": 60000},
                {"name": "Charlie", "dept": "IT", "salary": 70000},
            ]
        )

        result = df.filter("dept = 'IT'")

        assert result.count() == 2
        rows = result.collect()
        assert rows[0].name == "Alice"
        assert rows[0].dept == "IT"
        assert rows[1].name == "Charlie"
        assert rows[1].dept == "IT"

    def test_where_with_string_expression(self):
        """
        Test that where() (alias for filter()) also accepts string expressions.
        """
        spark = SparkSession("test_app")
        df = spark.createDataFrame(
            [
                {"name": "Alice", "dept": "IT", "salary": 50000},
                {"name": "Bob", "dept": "HR", "salary": 60000},
                {"name": "Charlie", "dept": "IT", "salary": 70000},
            ]
        )

        result = df.where("salary >= 60000")

        assert result.count() == 2
        rows = result.collect()
        assert rows[0].salary == 60000
        assert rows[1].salary == 70000

    def test_filter_with_column_expression_still_works(self):
        """
        Ensure that existing behavior of filter() with Column expressions
        is not regressed.
        """
        spark = SparkSession("test_app")
        df = spark.createDataFrame(
            [
                {"name": "Alice", "dept": "IT", "salary": 50000},
                {"name": "Bob", "dept": "HR", "salary": 60000},
                {"name": "Charlie", "dept": "IT", "salary": 70000},
            ]
        )

        # This should still work as before
        result = df.filter(df.salary > 55000)

        assert result.count() == 2
        rows = result.collect()
        assert rows[0].name == "Bob"
        assert rows[1].name == "Charlie"

    def test_filter_with_f_col_expression_still_works(self):
        """
        Ensure that filter() with F.col() expressions still works.
        """
        from sparkless.functions import F

        spark = SparkSession("test_app")
        df = spark.createDataFrame(
            [
                {"name": "Alice", "dept": "IT", "salary": 50000},
                {"name": "Bob", "dept": "HR", "salary": 60000},
                {"name": "Charlie", "dept": "IT", "salary": 70000},
            ]
        )

        # This should still work as before
        result = df.filter(F.col("salary") > 55000)

        assert result.count() == 2
        rows = result.collect()
        assert rows[0].name == "Bob"
        assert rows[1].name == "Charlie"

    def test_filter_with_string_and_condition(self):
        """
        Test filter with string expression using AND condition.
        """
        spark = SparkSession("test_app")
        df = spark.createDataFrame(
            [
                {"name": "Alice", "dept": "IT", "salary": 50000},
                {"name": "Bob", "dept": "HR", "salary": 60000},
                {"name": "Charlie", "dept": "IT", "salary": 70000},
            ]
        )

        result = df.filter("salary > 55000 AND dept = 'IT'")

        assert result.count() == 1
        rows = result.collect()
        assert rows[0].name == "Charlie"
        assert rows[0].dept == "IT"
        assert rows[0].salary == 70000
