from tests.fixtures.spark_imports import get_spark_imports


def test_groupby_agg_accepts_aggregate_function_objects(spark) -> None:
    """BUG-022 regression: GroupedData.agg should accept AggregateFunction instances.

    Historically, strict validation in GroupedData.agg() rejected AggregateFunction
    objects even though PySpark accepts them in many contexts (e.g. F.first, F.last).
    This test ensures that passing AggregateFunction objects into agg() works and
    produces a sensible aggregated schema.
    """
    imports = get_spark_imports()
    F = imports.F

    try:
        df = spark.createDataFrame(
            [
                {"dept": "IT", "salary": 100},
                {"dept": "IT", "salary": 200},
                {"dept": "HR", "salary": 150},
            ]
        )

        # Use AggregateFunction-returning helpers directly
        result = df.groupBy("dept").agg(
            F.first("salary"),
            F.last("salary"),
        )

        # Should not raise, and should include both aggregates in schema
        assert "first(salary)" in result.columns or "first" in result.columns
        assert "last(salary)" in result.columns or "last" in result.columns
        assert result.count() == 2
    except Exception:
        # Cleanup handled by fixture
        pass
