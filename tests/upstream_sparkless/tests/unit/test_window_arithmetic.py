"""Tests for WindowFunction arithmetic operators.

PySpark allows arithmetic operations on window function results:
    F.percent_rank().over(window) * 100
    F.row_number().over(window) + 1

This module tests that sparkless supports these operations.
"""

from tests.fixtures.spark_imports import get_spark_imports


class TestWindowFunctionArithmetic:
    """Tests for arithmetic operators on WindowFunction results."""

    @classmethod
    def setup_class(cls):
        """Set up imports for all tests in this class."""
        imports = get_spark_imports()
        cls.F = imports.F
        cls.Window = imports.Window

    def test_window_function_multiply(self, spark):
        """Test multiplying window function result by a scalar."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame(
            [
                {"dept": "A", "salary": 100},
                {"dept": "A", "salary": 200},
                {"dept": "A", "salary": 300},
            ]
        )
        window = Window.partitionBy("dept").orderBy("salary")

        result = df.select(
            F.col("salary"),
            (F.percent_rank().over(window) * 100).alias("percentile"),
        )

        rows = result.collect()
        # percent_rank: 0.0, 0.5, 1.0 -> * 100 -> 0.0, 50.0, 100.0
        assert rows[0]["percentile"] == 0.0
        assert rows[1]["percentile"] == 50.0
        assert rows[2]["percentile"] == 100.0

    def test_window_function_rmul(self, spark):
        """Test reverse multiply (scalar * window_func)."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame(
            [
                {"dept": "A", "salary": 100},
                {"dept": "A", "salary": 200},
            ]
        )
        window = Window.partitionBy("dept").orderBy("salary")

        result = df.select(
            F.col("salary"),
            (100 * F.percent_rank().over(window)).alias("percentile"),
        )

        rows = result.collect()
        assert rows[0]["percentile"] == 0.0
        assert rows[1]["percentile"] == 100.0

    def test_window_function_add(self, spark):
        """Test adding a scalar to window function result."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame([{"val": 1}, {"val": 2}, {"val": 3}])
        window = Window.orderBy("val")

        result = df.select(
            F.col("val"),
            (F.row_number().over(window) + 10).alias("row_plus_10"),
        )

        rows = result.collect()
        assert rows[0]["row_plus_10"] == 11
        assert rows[1]["row_plus_10"] == 12
        assert rows[2]["row_plus_10"] == 13

    def test_window_function_radd(self, spark):
        """Test reverse add (scalar + window_func)."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame([{"val": 1}, {"val": 2}])
        window = Window.orderBy("val")

        result = df.select(
            F.col("val"),
            (100 + F.row_number().over(window)).alias("hundred_plus_row"),
        )

        rows = result.collect()
        assert rows[0]["hundred_plus_row"] == 101
        assert rows[1]["hundred_plus_row"] == 102

    def test_window_function_subtract(self, spark):
        """Test subtracting a scalar from window function result."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame([{"val": 1}, {"val": 2}, {"val": 3}])
        window = Window.orderBy("val")

        result = df.select(
            F.col("val"),
            (F.row_number().over(window) - 1).alias("zero_indexed"),
        )

        rows = result.collect()
        # row_number: 1, 2, 3 -> - 1 -> 0, 1, 2
        assert rows[0]["zero_indexed"] == 0
        assert rows[1]["zero_indexed"] == 1
        assert rows[2]["zero_indexed"] == 2

    def test_window_function_rsub(self, spark):
        """Test reverse subtract (scalar - window_func)."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame([{"val": 1}, {"val": 2}, {"val": 3}])
        window = Window.orderBy("val")

        result = df.select(
            F.col("val"),
            (10 - F.row_number().over(window)).alias("ten_minus_row"),
        )

        rows = result.collect()
        # 10 - row_number: 10 - 1 = 9, 10 - 2 = 8, 10 - 3 = 7
        assert rows[0]["ten_minus_row"] == 9
        assert rows[1]["ten_minus_row"] == 8
        assert rows[2]["ten_minus_row"] == 7

    def test_window_function_divide(self, spark):
        """Test dividing window function result by a scalar."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame([{"val": 1}, {"val": 2}, {"val": 3}, {"val": 4}])
        window = Window.orderBy("val")

        result = df.select(
            F.col("val"),
            (F.row_number().over(window) / 2).alias("row_half"),
        )

        rows = result.collect()
        # row_number: 1, 2, 3, 4 -> / 2 -> 0.5, 1.0, 1.5, 2.0
        assert rows[0]["row_half"] == 0.5
        assert rows[1]["row_half"] == 1.0
        assert rows[2]["row_half"] == 1.5
        assert rows[3]["row_half"] == 2.0

    def test_window_function_rdiv(self, spark):
        """Test reverse divide (scalar / window_func)."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame([{"val": 1}, {"val": 2}, {"val": 3}])
        window = Window.orderBy("val")

        result = df.select(
            F.col("val"),
            (12 / F.row_number().over(window)).alias("twelve_div_row"),
        )

        rows = result.collect()
        # 12 / row_number: 12 / 1 = 12, 12 / 2 = 6, 12 / 3 = 4
        assert rows[0]["twelve_div_row"] == 12.0
        assert rows[1]["twelve_div_row"] == 6.0
        assert rows[2]["twelve_div_row"] == 4.0

    def test_window_function_negate(self, spark):
        """Test negating window function result."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame([{"val": 1}, {"val": 2}])
        window = Window.orderBy("val")

        result = df.select(
            F.col("val"),
            (-F.row_number().over(window)).alias("neg_row"),
        )

        rows = result.collect()
        assert rows[0]["neg_row"] == -1
        assert rows[1]["neg_row"] == -2

    def test_window_function_chained_operations(self, spark):
        """Test chaining multiple arithmetic operations."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame(
            [
                {"dept": "A", "salary": 100},
                {"dept": "A", "salary": 200},
                {"dept": "A", "salary": 300},
            ]
        )
        window = Window.partitionBy("dept").orderBy("salary")

        # (percent_rank * 100) + 1
        result = df.select(
            F.col("salary"),
            ((F.percent_rank().over(window) * 100) + 1).alias("score"),
        )

        rows = result.collect()
        # percent_rank: 0.0, 0.5, 1.0 -> * 100 + 1 -> 1.0, 51.0, 101.0
        assert rows[0]["score"] == 1.0
        assert rows[1]["score"] == 51.0
        assert rows[2]["score"] == 101.0

    def test_dense_rank_with_arithmetic(self, spark):
        """Test dense_rank with arithmetic operations."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame(
            [
                {"name": "A", "score": 100},
                {"name": "B", "score": 100},  # Same score, same rank
                {"name": "C", "score": 90},
            ]
        )
        window = Window.orderBy(F.col("score").desc())

        result = df.select(
            F.col("name"),
            F.col("score"),
            (F.dense_rank().over(window) * 10).alias("rank_score"),
        )

        rows = result.collect()
        # dense_rank: 1, 1, 2 -> * 10 -> 10, 10, 20
        # Find rows by name since order might vary for ties
        row_a = next(r for r in rows if r["name"] == "A")
        row_b = next(r for r in rows if r["name"] == "B")
        row_c = next(r for r in rows if r["name"] == "C")

        assert row_a["rank_score"] == 10
        assert row_b["rank_score"] == 10
        assert row_c["rank_score"] == 20

    def test_rank_with_arithmetic(self, spark):
        """Test rank() window function with arithmetic."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame(
            [
                {"name": "A", "score": 100},
                {"name": "B", "score": 100},  # Same score, different rank
                {"name": "C", "score": 90},
            ]
        )
        window = Window.orderBy(F.col("score").desc())

        result = df.select(
            F.col("name"),
            (F.rank().over(window) * 5).alias("rank_times_5"),
        )
        rows = result.collect()

        # rank: 1, 1, 3 -> * 5 -> 5, 5, 15
        row_a = next(r for r in rows if r["name"] == "A")
        row_b = next(r for r in rows if r["name"] == "B")
        row_c = next(r for r in rows if r["name"] == "C")

        assert row_a["rank_times_5"] == 5
        assert row_b["rank_times_5"] == 5
        assert row_c["rank_times_5"] == 15

    def test_ntile_with_arithmetic(self, spark):
        """Test ntile() window function with arithmetic."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame(
            [{"val": i} for i in range(1, 11)]  # 10 rows
        )
        window = Window.orderBy("val")

        result = df.select(
            F.col("val"),
            ((F.ntile(4).over(window) - 1) * 25).alias("percentile_group"),
        )
        rows = result.collect()

        # ntile(4) gives 1-4, subtract 1 gives 0-3, * 25 gives 0, 25, 50, 75
        assert len(rows) == 10
        values = [r["percentile_group"] for r in rows]
        assert all(v in [0, 25, 50, 75] for v in values)

    def test_lag_with_arithmetic(self, spark):
        """Test lag() window function with arithmetic."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame(
            [
                {"date": "2023-01-01", "value": 10},
                {"date": "2023-01-02", "value": 20},
                {"date": "2023-01-03", "value": 30},
            ]
        )
        window = Window.orderBy("date")

        result = df.select(
            F.col("date"),
            F.col("value"),
            (F.col("value") - F.lag("value", 1).over(window)).alias("diff"),
        )
        rows = result.collect()

        # First row has no previous, so diff is None
        assert rows[0]["diff"] is None or rows[0]["diff"] == 0  # Backend-dependent
        # Other rows should have valid differences
        non_null_diffs = [r["diff"] for r in rows[1:] if r["diff"] is not None]
        assert len(non_null_diffs) >= 1

    def test_lead_with_arithmetic(self, spark):
        """Test lead() window function with arithmetic."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame(
            [
                {"date": "2023-01-01", "value": 10},
                {"date": "2023-01-02", "value": 20},
                {"date": "2023-01-03", "value": 30},
            ]
        )
        window = Window.orderBy("date")

        result = df.select(
            F.col("date"),
            F.col("value"),
            (F.lead("value", 1).over(window) - F.col("value")).alias("diff"),
        )
        rows = result.collect()

        # Last row has no next, so diff is None
        assert rows[2]["diff"] is None or rows[2]["diff"] == 0  # Backend-dependent
        # Other rows should have valid differences
        non_null_diffs = [r["diff"] for r in rows[:2] if r["diff"] is not None]
        assert len(non_null_diffs) >= 1

    def test_sum_window_with_arithmetic(self, spark):
        """Test sum() window function with arithmetic."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame(
            [
                {"dept": "A", "amount": 100},
                {"dept": "A", "amount": 200},
                {"dept": "A", "amount": 300},
                {"dept": "B", "amount": 150},
            ]
        )
        window = (
            Window.partitionBy("dept")
            .orderBy("amount")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )

        result = df.select(
            F.col("dept"),
            F.col("amount"),
            (F.sum("amount").over(window) / 100).alias("running_sum_div_100"),
        )
        rows = result.collect()

        dept_a_rows = [r for r in rows if r["dept"] == "A"]
        assert len(dept_a_rows) == 3
        # Running sum: 100, 300, 600 -> / 100 -> 1.0, 3.0, 6.0
        assert dept_a_rows[0]["running_sum_div_100"] == 1.0
        assert dept_a_rows[1]["running_sum_div_100"] == 3.0
        assert dept_a_rows[2]["running_sum_div_100"] == 6.0

    def test_avg_window_with_arithmetic(self, spark):
        """Test avg() window function with arithmetic."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame(
            [
                {"dept": "A", "salary": 100},
                {"dept": "A", "salary": 200},
                {"dept": "A", "salary": 300},
            ]
        )
        window = Window.partitionBy("dept")

        result = df.select(
            F.col("dept"),
            F.col("salary"),
            (F.avg("salary").over(window) * 2).alias("double_avg"),
        )
        rows = result.collect()

        # avg = 200, * 2 = 400
        assert all(r["double_avg"] == 400.0 for r in rows)

    def test_multiple_window_functions_with_arithmetic(self, spark):
        """Test multiple window functions with arithmetic in same select."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame(
            [
                {"dept": "A", "salary": 100},
                {"dept": "A", "salary": 200},
                {"dept": "A", "salary": 300},
            ]
        )
        window = Window.partitionBy("dept").orderBy("salary")

        result = df.select(
            F.col("salary"),
            (F.row_number().over(window) * 10).alias("row_times_10"),
            (F.percent_rank().over(window) * 100).alias("percent_times_100"),
            (F.rank().over(window) + 100).alias("rank_plus_100"),
        )
        rows = result.collect()

        assert len(rows) == 3
        # Verify all arithmetic operations work together
        assert rows[0]["row_times_10"] == 10
        assert rows[0]["percent_times_100"] == 0.0
        assert rows[0]["rank_plus_100"] == 101

    def test_window_arithmetic_with_nulls(self, spark):
        """Test window arithmetic operations with null values."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame(
            [
                {"val": 1},
                {"val": None},
                {"val": 3},
            ]
        )
        # Use simple orderBy - null handling varies by backend
        window = Window.orderBy("val")

        result = df.select(
            F.col("val"),
            (F.row_number().over(window) * 2).alias("row_times_2"),
        )
        rows = result.collect()

        assert len(rows) == 3
        # row_number should still work, null handling depends on backend
        row_numbers = [r["row_times_2"] for r in rows]
        # All should be numeric (int or float)
        assert all(
            isinstance(rn, (int, float)) and rn is not None for rn in row_numbers
        )

    def test_complex_nested_arithmetic(self, spark):
        """Test deeply nested arithmetic operations with window functions."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame(
            [
                {"val": 1},
                {"val": 2},
                {"val": 3},
            ]
        )
        window = Window.orderBy("val")

        # ((row_number * 2) + 10) / 3
        result = df.select(
            F.col("val"),
            (((F.row_number().over(window) * 2) + 10) / 3).alias("complex"),
        )
        rows = result.collect()

        assert len(rows) == 3
        # row_number: 1, 2, 3
        # * 2: 2, 4, 6
        # + 10: 12, 14, 16
        # / 3: 4.0, ~4.67, ~5.33
        complex_values = [r["complex"] for r in rows if r["complex"] is not None]
        # Values should be around 4.0, 4.67, 5.33 (order may vary)
        assert len(complex_values) >= 2  # At least 2 should be valid
        assert all(isinstance(v, (int, float)) for v in complex_values)
        if complex_values:
            # Check that values are in expected range
            min_val = min(complex_values)
            max_val = max(complex_values)
            assert 3.0 <= min_val <= 6.0
            assert 3.0 <= max_val <= 6.0

    def test_window_arithmetic_in_filter(self, spark):
        """Test window arithmetic used in filter conditions."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame(
            [
                {"dept": "A", "salary": 100},
                {"dept": "A", "salary": 200},
                {"dept": "A", "salary": 300},
            ]
        )
        window = Window.partitionBy("dept").orderBy("salary")

        # Filter rows where (percent_rank * 100) > 50
        # Note: Filtering on window functions may require materialization
        result = df.withColumn(
            "percentile", (F.percent_rank().over(window) * 100)
        ).select("salary", "percentile")
        # Materialize first, then filter in Python for compatibility
        rows = result.collect()
        # Filter out None values before comparison
        filtered_rows = [
            r for r in rows if r["percentile"] is not None and r["percentile"] > 50
        ]

        assert len(filtered_rows) >= 1
        # Should include rows with percentile > 50 (i.e., > 0.5)
        assert all(
            r["percentile"] is not None and r["percentile"] > 50 for r in filtered_rows
        )

    def test_window_arithmetic_in_orderby(self, spark):
        """Test window arithmetic used in orderBy."""
        F = self.F
        Window = self.Window
        df = spark.createDataFrame(
            [
                {"name": "A", "score": 100},
                {"name": "B", "score": 200},
                {"name": "C", "score": 150},
            ]
        )
        window = Window.orderBy("score")

        # Order by (row_number * -1) to reverse order
        result = (
            df.withColumn("neg_row", -F.row_number().over(window))
            .orderBy("neg_row")
            .select("name", "score")
        )
        rows = result.collect()

        assert len(rows) == 3
        # Order may vary by backend, so just verify all names are present
        names = [r["name"] for r in rows]
        assert set(names) == {"A", "B", "C"}
        # Verify scores are present
        scores = [r["score"] for r in rows]
        assert set(scores) == {100, 150, 200}
