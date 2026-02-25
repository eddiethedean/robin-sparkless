#!/usr/bin/env python3
"""
Script to verify which skipped tests actually work.

This script attempts to run skipped tests to see if the implementation
actually exists and works, helping identify tests that are incorrectly skipped.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from sparkless import SparkSession, F  # noqa: E402


def test_left_join():
    """Test if left join works."""
    try:
        spark = SparkSession("test")
        df1 = spark.createDataFrame([{"id": 1, "name": "A"}, {"id": 2, "name": "B"}])
        df2 = spark.createDataFrame([{"id": 1, "val": "X"}])
        result = df1.join(df2, df1.id == df2.id, "left")
        rows = result.collect()
        return len(rows) > 0, f"Got {len(rows)} rows"
    except Exception as e:
        return False, str(e)


def test_right_join():
    """Test if right join works."""
    try:
        spark = SparkSession("test")
        df1 = spark.createDataFrame([{"id": 1, "name": "A"}])
        df2 = spark.createDataFrame([{"id": 1, "val": "X"}, {"id": 2, "val": "Y"}])
        result = df1.join(df2, df1.id == df2.id, "right")
        rows = result.collect()
        return len(rows) > 0, f"Got {len(rows)} rows"
    except Exception as e:
        return False, str(e)


def test_outer_join():
    """Test if outer join works."""
    try:
        spark = SparkSession("test")
        df1 = spark.createDataFrame([{"id": 1, "name": "A"}, {"id": 2, "name": "B"}])
        df2 = spark.createDataFrame([{"id": 1, "val": "X"}, {"id": 3, "val": "Z"}])
        result = df1.join(df2, df1.id == df2.id, "outer")
        rows = result.collect()
        return len(rows) > 0, f"Got {len(rows)} rows"
    except Exception as e:
        return False, str(e)


def test_day_function():
    """Test if day function works."""
    try:
        spark = SparkSession("test")
        df = spark.createDataFrame([{"date": "2020-01-15"}])
        result = df.select(F.day(df.date))
        rows = result.collect()
        return len(rows) > 0 and rows[0][
            0
        ] is not None, f"Got {rows[0][0] if rows else 'None'}"
    except Exception as e:
        return False, str(e)


def test_window_functions():
    """Test if window functions work."""
    try:
        from sparkless.window import Window

        spark = SparkSession("test")
        df = spark.createDataFrame(
            [
                {"id": 1, "name": "Alice", "dept": "IT", "salary": 50000},
                {"id": 2, "name": "Bob", "dept": "HR", "salary": 60000},
            ]
        )
        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("row_num", F.row_number().over(window_spec))
        rows = result.collect()
        return len(rows) > 0, f"Got {len(rows)} rows"
    except Exception as e:
        return False, str(e)


def test_select_with_alias():
    """Test if select with alias works."""
    try:
        spark = SparkSession("test")
        df = spark.createDataFrame([{"id": 1, "name": "A"}])
        result = df.select(df.id.alias("employee_id"), df.name.alias("employee_name"))
        rows = result.collect()
        return len(rows) > 0, f"Got {len(rows)} rows"
    except Exception as e:
        return False, str(e)


def test_filter_with_boolean():
    """Test if filter with boolean works."""
    try:
        spark = SparkSession("test")
        df = spark.createDataFrame(
            [
                {"age": 30, "department": "IT"},
                {"age": 20, "department": "HR"},
            ]
        )
        result = df.filter((df.age > 25) & (df.department == "IT"))
        rows = result.collect()
        return len(rows) > 0, f"Got {len(rows)} rows"
    except Exception as e:
        return False, str(e)


def test_drop_column():
    """Test if drop column works."""
    try:
        spark = SparkSession("test")
        df = spark.createDataFrame([{"id": 1, "name": "A", "age": 30}])
        result = df.drop("age")
        rows = result.collect()
        cols = result.columns
        return "age" not in cols and len(rows) > 0, f"Columns: {cols}"
    except Exception as e:
        return False, str(e)


def main():
    """Run all verification tests."""
    tests = [
        ("Left Join", test_left_join),
        ("Right Join", test_right_join),
        ("Outer Join", test_outer_join),
        ("Day Function", test_day_function),
        ("Window Functions", test_window_functions),
        ("Select with Alias", test_select_with_alias),
        ("Filter with Boolean", test_filter_with_boolean),
        ("Drop Column", test_drop_column),
    ]

    print("Verifying skipped tests...")
    print("=" * 60)

    working = []
    broken = []

    for name, test_func in tests:
        success, message = test_func()
        status = "✓ WORKS" if success else "✗ BROKEN"
        print(f"{status:12} {name:30} - {message}")

        if success:
            working.append(name)
        else:
            broken.append((name, message))

    print("=" * 60)
    print("\nSummary:")
    print(f"  Working: {len(working)}/{len(tests)}")
    print(f"  Broken: {len(broken)}/{len(tests)}")

    if working:
        print("\n✓ These tests can have skip markers removed:")
        for name in working:
            print(f"  - {name}")

    if broken:
        print("\n✗ These tests need implementation:")
        for name, msg in broken:
            print(f"  - {name}: {msg}")

    return 0 if len(working) > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
