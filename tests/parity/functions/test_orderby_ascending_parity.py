"""
PySpark parity tests for Issue #327: orderBy() with .asc()/.desc().

PySpark does not support orderBy(..., ascending=...). Use col("x").asc() or .desc().
"""


class TestOrderByAscendingParity:
    """PySpark parity tests for orderBy() with column.asc()/desc()."""

    def test_orderby_ascending_true_parity(self, spark, spark_imports):
        """Test orderBy(col.asc()) matches PySpark."""
        F = spark_imports.F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "StringValue": "AAA"},
                {"Name": "Bob", "StringValue": "ZZZ"},
                {"Name": "Charlie", "StringValue": "MMM"},
            ]
        )

        result = df.orderBy(F.col("StringValue").asc())
        rows = result.collect()

        assert len(rows) == 3
        assert rows[0]["StringValue"] == "AAA"
        assert rows[1]["StringValue"] == "MMM"
        assert rows[2]["StringValue"] == "ZZZ"

    def test_orderby_ascending_false_parity(self, spark, spark_imports):
        """Test orderBy(col.desc()) matches PySpark."""
        F = spark_imports.F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "StringValue": "AAA"},
                {"Name": "Bob", "StringValue": "ZZZ"},
                {"Name": "Charlie", "StringValue": "MMM"},
            ]
        )

        result = df.orderBy(F.col("StringValue").desc())
        rows = result.collect()

        assert len(rows) == 3
        assert rows[0]["StringValue"] == "ZZZ"
        assert rows[1]["StringValue"] == "MMM"
        assert rows[2]["StringValue"] == "AAA"

    def test_sort_with_ascending_parity(self, spark, spark_imports):
        """Test sort(col.desc()) matches PySpark."""
        F = spark_imports.F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 10},
                {"Name": "Bob", "Value": 5},
                {"Name": "Charlie", "Value": 20},
            ]
        )

        result = df.sort(F.col("Value").desc())
        rows = result.collect()

        assert len(rows) == 3
        assert rows[0]["Value"] == 20
        assert rows[1]["Value"] == 10
        assert rows[2]["Value"] == 5
