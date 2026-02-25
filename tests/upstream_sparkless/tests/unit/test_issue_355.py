"""
Test for issue #355: unionByName produces incorrect results when same DataFrame
is used in two join branches (diamond dependency).

https://github.com/eddiethedean/sparkless/issues/355
"""

from sparkless.sql import SparkSession
from sparkless import functions as F


class TestIssue355DiamondDependency:
    """Test unionByName with diamond dependency (same DataFrame in two branches)."""

    def test_unionByName_diamond_dependency(self):
        """Test that unionByName correctly handles diamond dependencies.

        When the same DataFrame is used as input to two separate join operations
        (creating a diamond/fork dependency graph), and the results are then
        combined via unionByName, sparkless should produce the correct result
        without duplicating rows.
        """
        spark = SparkSession("test")

        existing = spark.createDataFrame(
            [(1, "a", 100), (2, "b", 200), (3, "c", 300)],
            ["id", "name", "value"],
        )

        source = spark.createDataFrame(
            [(1, "a", 150), (2, "b", 250)],
            ["id", "name", "value"],
        )

        # Anti-join: existing rows NOT in source
        sk = source.select("id").distinct().withColumn("_m", F.lit(True))
        branch_a = (
            existing.join(sk, on=["id"], how="left")
            .filter(F.col("_m").isNull())
            .drop("_m")
        )

        # Inner-join: existing rows that ARE in source
        branch_b = existing.join(source.select("id").distinct(), on=["id"], how="inner")

        # Union both branches
        combined = branch_a.unionByName(branch_b, allowMissingColumns=True)
        result = combined.collect()

        # Expected: 3 rows total
        # - 1 row from branch_a (id=3, not in source)
        # - 2 rows from branch_b (id=1 and id=2, matched in source)
        assert len(result) == 3, f"Expected 3 rows, got {len(result)}"

        # Verify we have the correct rows
        ids = {row.id for row in result}
        assert ids == {1, 2, 3}, f"Expected ids {{1, 2, 3}}, got {ids}"

        # Verify values are correct (not duplicated original values)
        id_to_value = {row.id: row.value for row in result}
        assert id_to_value[1] == 100, (
            f"Expected value 100 for id=1, got {id_to_value[1]}"
        )
        assert id_to_value[2] == 200, (
            f"Expected value 200 for id=2, got {id_to_value[2]}"
        )
        assert id_to_value[3] == 300, (
            f"Expected value 300 for id=3, got {id_to_value[3]}"
        )

        # Verify no duplicates
        assert len(ids) == 3, "Found duplicate IDs in result"

    def test_unionByName_diamond_dependency_with_filters(self):
        """Test diamond dependency with filter operations in branches."""
        spark = SparkSession("test")

        base = spark.createDataFrame(
            [(1, "a", 10), (2, "b", 20), (3, "c", 30), (4, "d", 40)],
            ["id", "name", "value"],
        )

        # Branch A: Filter for even IDs
        branch_a = base.filter(F.col("id") % 2 == 0)

        # Branch B: Filter for odd IDs
        branch_b = base.filter(F.col("id") % 2 == 1)

        # Union both branches
        result = branch_a.unionByName(branch_b).orderBy("id").collect()

        # Should have all 4 rows
        assert len(result) == 4, f"Expected 4 rows, got {len(result)}"
        ids = {row.id for row in result}
        assert ids == {1, 2, 3, 4}, f"Expected ids {{1, 2, 3, 4}}, got {ids}"

    def test_unionByName_diamond_dependency_with_select(self):
        """Test diamond dependency with select operations in branches."""
        spark = SparkSession("test")

        base = spark.createDataFrame(
            [(1, "a", 100), (2, "b", 200), (3, "c", 300)],
            ["id", "name", "value"],
        )

        # Branch A: Select id and value
        branch_a = base.select("id", "value").withColumn("source", F.lit("A"))

        # Branch B: Select id and name
        branch_b = base.select("id", "name").withColumn("source", F.lit("B"))

        # Union with allowMissingColumns
        result = (
            branch_a.unionByName(branch_b, allowMissingColumns=True)
            .orderBy("id", "source")
            .collect()
        )

        # Should have 6 rows (3 from each branch)
        assert len(result) == 6, f"Expected 6 rows, got {len(result)}"

        # Verify all IDs appear twice (once in each branch)
        id_counts = {}
        for row in result:
            id_counts[row.id] = id_counts.get(row.id, 0) + 1
        assert all(count == 2 for count in id_counts.values()), (
            "Each ID should appear exactly twice"
        )

    def test_unionByName_diamond_dependency_with_withColumn(self):
        """Test diamond dependency with withColumn operations in branches."""
        spark = SparkSession("test")

        base = spark.createDataFrame(
            [(1, 10), (2, 20), (3, 30)],
            ["id", "value"],
        )

        # Branch A: Add column 'doubled'
        branch_a = base.withColumn("doubled", F.col("value") * 2)

        # Branch B: Add column 'tripled'
        branch_b = base.withColumn("tripled", F.col("value") * 3)

        # Union with allowMissingColumns
        result = (
            branch_a.unionByName(branch_b, allowMissingColumns=True)
            .orderBy("id")
            .collect()
        )

        # Should have 6 rows (3 from each branch)
        assert len(result) == 6, f"Expected 6 rows, got {len(result)}"

        # Verify values are correct
        for row in result:
            if hasattr(row, "doubled") and row.doubled is not None:
                assert row.doubled == row.value * 2, (
                    f"Expected doubled={row.value * 2}, got {row.doubled}"
                )
            if hasattr(row, "tripled") and row.tripled is not None:
                assert row.tripled == row.value * 3, (
                    f"Expected tripled={row.value * 3}, got {row.tripled}"
                )

    def test_unionByName_diamond_dependency_three_branches(self):
        """Test diamond dependency with three branches."""
        spark = SparkSession("test")

        base = spark.createDataFrame(
            [(1, "a"), (2, "b"), (3, "c"), (4, "d")],
            ["id", "name"],
        )

        # Three branches with different filters
        branch_a = base.filter(F.col("id") < 2).withColumn("branch", F.lit("A"))
        branch_b = base.filter((F.col("id") >= 2) & (F.col("id") < 4)).withColumn(
            "branch", F.lit("B")
        )
        branch_c = base.filter(F.col("id") >= 4).withColumn("branch", F.lit("C"))

        # Union all three branches
        result = (
            branch_a.unionByName(branch_b).unionByName(branch_c).orderBy("id").collect()
        )

        # Should have all 4 rows
        assert len(result) == 4, f"Expected 4 rows, got {len(result)}"
        ids = {row.id for row in result}
        assert ids == {1, 2, 3, 4}, f"Expected ids {{1, 2, 3, 4}}, got {ids}"

    def test_unionByName_diamond_dependency_nested_transformations(self):
        """Test diamond dependency with nested/chain transformations."""
        spark = SparkSession("test")

        base = spark.createDataFrame(
            [(1, "a", 10), (2, "b", 20), (3, "c", 30)],
            ["id", "name", "value"],
        )

        # Branch A: Multiple chained transformations
        branch_a = (
            base.filter(F.col("id") <= 2)
            .withColumn("multiplied", F.col("value") * 2)
            .select("id", "name", "multiplied")
            .withColumnRenamed("multiplied", "result")
        )

        # Branch B: Different chain of transformations
        branch_b = (
            base.filter(F.col("id") > 2)
            .withColumn("added", F.col("value") + 10)
            .select("id", "name", "added")
            .withColumnRenamed("added", "result")
        )

        # Union both branches
        result = branch_a.unionByName(branch_b).orderBy("id").collect()

        # Should have all 3 rows
        assert len(result) == 3, f"Expected 3 rows, got {len(result)}"
        ids = {row.id for row in result}
        assert ids == {1, 2, 3}, f"Expected ids {{1, 2, 3}}, got {ids}"

        # Verify results are correct
        for row in result:
            if row.id <= 2:
                assert row.result == row.id * 10 * 2, (
                    f"Expected result={row.id * 10 * 2} for id={row.id}, got {row.result}"
                )
            else:
                assert row.result == row.id * 10 + 10, (
                    f"Expected result={row.id * 10 + 10} for id={row.id}, got {row.result}"
                )

    def test_unionByName_diamond_dependency_with_aggregations(self):
        """Test diamond dependency where branches perform aggregations."""
        spark = SparkSession("test")

        base = spark.createDataFrame(
            [(1, "A", 10), (1, "A", 20), (2, "B", 30), (2, "B", 40)],
            ["id", "category", "value"],
        )

        # Branch A: Group by id, sum values
        branch_a = base.groupBy("id").agg(F.sum("value").alias("total"))

        # Branch B: Group by category, avg values
        branch_b = base.groupBy("category").agg(F.avg("value").alias("average"))

        # Union with allowMissingColumns
        result = (
            branch_a.unionByName(branch_b, allowMissingColumns=True)
            .orderBy("id")
            .collect()
        )

        # Should have 4 rows (2 from branch_a grouped by id, 2 from branch_b grouped by category)
        assert len(result) == 4, f"Expected 4 rows, got {len(result)}"

        # Verify branch_a results (grouped by id)
        id_results = [row for row in result if row.id is not None]
        assert len(id_results) == 2, "Should have 2 rows from branch_a"
        id_to_total = {row.id: row.total for row in id_results}
        assert id_to_total[1] == 30, f"Expected total=30 for id=1, got {id_to_total[1]}"
        assert id_to_total[2] == 70, f"Expected total=70 for id=2, got {id_to_total[2]}"

        # Verify branch_b results (grouped by category)
        category_results = [row for row in result if row.category is not None]
        assert len(category_results) == 2, "Should have 2 rows from branch_b"
        category_to_avg = {row.category: row.average for row in category_results}
        assert category_to_avg["A"] == 15.0, (
            f"Expected average=15.0 for category=A, got {category_to_avg['A']}"
        )
        assert category_to_avg["B"] == 35.0, (
            f"Expected average=35.0 for category=B, got {category_to_avg['B']}"
        )

    def test_unionByName_diamond_dependency_empty_branch(self):
        """Test diamond dependency where one branch is empty."""
        spark = SparkSession("test")

        base = spark.createDataFrame(
            [(1, "a"), (2, "b"), (3, "c")],
            ["id", "name"],
        )

        # Branch A: Filter that returns all rows
        branch_a = base.filter(F.col("id") > 0)

        # Branch B: Filter that returns no rows
        branch_b = base.filter(F.col("id") < 0)

        # Union both branches
        result = branch_a.unionByName(branch_b).collect()

        # Should have 3 rows (only from branch_a)
        assert len(result) == 3, f"Expected 3 rows, got {len(result)}"
        ids = {row.id for row in result}
        assert ids == {1, 2, 3}, f"Expected ids {{1, 2, 3}}, got {ids}"

    def test_unionByName_diamond_dependency_single_row(self):
        """Test diamond dependency with single row DataFrame."""
        spark = SparkSession("test")

        base = spark.createDataFrame([(1, "a", 100)], ["id", "name", "value"])

        # Branch A: Keep as is
        branch_a = base.withColumn("source", F.lit("A"))

        # Branch B: Transform
        branch_b = base.withColumn("source", F.lit("B")).withColumn(
            "doubled", F.col("value") * 2
        )

        # Union with allowMissingColumns
        result = branch_a.unionByName(branch_b, allowMissingColumns=True).collect()

        # Should have 2 rows
        assert len(result) == 2, f"Expected 2 rows, got {len(result)}"

    def test_unionByName_diamond_dependency_complex_expressions(self):
        """Test diamond dependency with complex column expressions."""
        spark = SparkSession("test")

        base = spark.createDataFrame(
            [(1, 10, 5), (2, 20, 10), (3, 30, 15)],
            ["id", "value1", "value2"],
        )

        # Branch A: Complex expression with value1
        branch_a = base.withColumn(
            "computed", F.col("value1") * 2 + F.col("value2")
        ).select("id", "computed")

        # Branch B: Complex expression with value2
        branch_b = base.withColumn(
            "computed", F.col("value2") * 3 - F.col("value1")
        ).select("id", "computed")

        # Union both branches
        result = branch_a.unionByName(branch_b).orderBy("id").collect()

        # Should have 6 rows (3 from each branch)
        assert len(result) == 6, f"Expected 6 rows, got {len(result)}"

        # Verify computations are correct
        for row in result:
            if row.id == 1:
                # branch_a: 10*2+5=25, branch_b: 5*3-10=5
                assert row.computed in [25, 5], (
                    f"Unexpected computed value {row.computed} for id=1"
                )

    def test_unionByName_diamond_dependency_with_drop(self):
        """Test diamond dependency with drop operations in branches."""
        spark = SparkSession("test")

        base = spark.createDataFrame(
            [(1, "a", 10, "x"), (2, "b", 20, "y"), (3, "c", 30, "z")],
            ["id", "name", "value", "extra"],
        )

        # Branch A: Drop 'extra' column
        branch_a = base.drop("extra")

        # Branch B: Drop 'name' column
        branch_b = base.drop("name")

        # Union with allowMissingColumns
        result = (
            branch_a.unionByName(branch_b, allowMissingColumns=True)
            .orderBy("id")
            .collect()
        )

        # Should have 6 rows (3 from each branch)
        assert len(result) == 6, f"Expected 6 rows, got {len(result)}"

    def test_unionByName_diamond_dependency_multiple_unions(self):
        """Test diamond dependency with multiple union operations."""
        spark = SparkSession("test")

        base = spark.createDataFrame(
            [(1, "a"), (2, "b"), (3, "c"), (4, "d")],
            ["id", "name"],
        )

        # Create multiple branches
        branch_a = base.filter(F.col("id") == 1)
        branch_b = base.filter(F.col("id") == 2)
        branch_c = base.filter(F.col("id") == 3)
        branch_d = base.filter(F.col("id") == 4)

        # Chain multiple unions
        result = (
            branch_a.unionByName(branch_b)
            .unionByName(branch_c)
            .unionByName(branch_d)
            .orderBy("id")
            .collect()
        )

        # Should have all 4 rows
        assert len(result) == 4, f"Expected 4 rows, got {len(result)}"
        ids = {row.id for row in result}
        assert ids == {1, 2, 3, 4}, f"Expected ids {{1, 2, 3, 4}}, got {ids}"

    def test_unionByName_diamond_dependency_with_window_functions(self):
        """Test diamond dependency with window functions in branches."""
        spark = SparkSession("test")

        base = spark.createDataFrame(
            [(1, "A", 10), (1, "A", 20), (2, "B", 30), (2, "B", 40)],
            ["id", "category", "value"],
        )

        # Branch A: Window function - row_number
        from sparkless.window import Window

        branch_a = base.withColumn(
            "row_num", F.row_number().over(Window.partitionBy("id").orderBy("value"))
        )

        # Branch B: Window function - rank
        branch_b = base.withColumn(
            "rank_val", F.rank().over(Window.partitionBy("category").orderBy("value"))
        )

        # Union with allowMissingColumns
        result = (
            branch_a.unionByName(branch_b, allowMissingColumns=True)
            .orderBy("id", "value")
            .collect()
        )

        # Should have 8 rows (4 from each branch)
        assert len(result) == 8, f"Expected 8 rows, got {len(result)}"

    def test_unionByName_diamond_dependency_preserves_original_data(self):
        """Test that original DataFrame is not modified by union operations."""
        spark = SparkSession("test")

        base = spark.createDataFrame(
            [(1, "a", 100), (2, "b", 200)],
            ["id", "name", "value"],
        )

        # Create branches
        branch_a = base.filter(F.col("id") == 1).withColumn("branch", F.lit("A"))
        branch_b = base.filter(F.col("id") == 2).withColumn("branch", F.lit("B"))

        # Perform union
        unioned = branch_a.unionByName(branch_b)

        # Verify original base is unchanged
        base_rows = base.collect()
        assert len(base_rows) == 2, "Original DataFrame should be unchanged"
        assert base_rows[0].value == 100, (
            "Original DataFrame values should be unchanged"
        )
        assert base_rows[1].value == 200, (
            "Original DataFrame values should be unchanged"
        )

        # Verify union result is correct
        union_rows = unioned.collect()
        assert len(union_rows) == 2, "Union should have 2 rows"
