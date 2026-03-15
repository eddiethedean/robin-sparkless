"""
Parity tests for slice() with negative start index (#1477).

PySpark: negative start counts from the end (-1 = last, -2 = second-to-last).
slice(arr, -2, 2) should return the last 2 elements, not the first 2.
"""

from tests.tools.parity_base import ParityTestBase
from sparkless.testing import get_imports


class TestSliceNegativeStartParity(ParityTestBase):
    """Test slice() negative start matches PySpark (#1477)."""

    def test_slice_positive_start(self, spark):
        """slice(arr, 1, 3) returns first 3 elements."""
        imports = get_imports()
        F = imports.F

        df = spark.createDataFrame([{"arr": [1, 2, 3, 4, 5]}])
        result = df.select(F.slice("arr", 1, 3).alias("first_3")).collect()
        assert len(result) == 1
        assert result[0]["first_3"] == [1, 2, 3]

    def test_slice_negative_start_last_two(self, spark):
        """slice(arr, -2, 2) returns last 2 elements [4, 5], not [1, 2]."""
        imports = get_imports()
        F = imports.F

        df = spark.createDataFrame([{"arr": [1, 2, 3, 4, 5]}])
        result = df.select(F.slice("arr", -2, 2).alias("last_2")).collect()
        assert len(result) == 1
        assert result[0]["last_2"] == [4, 5]

    def test_slice_negative_one_last_element(self, spark):
        """slice(arr, -1, 1) returns last element."""
        imports = get_imports()
        F = imports.F

        df = spark.createDataFrame([{"arr": [10, 20, 30]}])
        result = df.select(F.slice("arr", -1, 1).alias("last")).collect()
        assert len(result) == 1
        assert result[0]["last"] == [30]
