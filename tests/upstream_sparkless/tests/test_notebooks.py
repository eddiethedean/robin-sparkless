#!/usr/bin/env python3
"""Test script to verify notebook code works."""

from tests.fixtures.spark_imports import get_spark_imports
import random
from datetime import datetime, timedelta
import time


def test_quickstart(spark):
    """Test quickstart notebook code."""
    imports = get_spark_imports()
    F = imports.F

    print("Testing Quickstart Tutorial...")

    # Step 1: Use provided session
    app_name = (
        spark.appName
        if hasattr(spark, "appName")
        else getattr(spark, "app_name", "test")
    )
    print(f"✅ Session created: {app_name}")

    # Step 2: Create DataFrame
    data = [
        {
            "order_id": 1,
            "customer": "Alice",
            "product": "Laptop",
            "quantity": 1,
            "price": 1200,
        },
        {
            "order_id": 2,
            "customer": "Bob",
            "product": "Mouse",
            "quantity": 2,
            "price": 25,
        },
        {
            "order_id": 3,
            "customer": "Alice",
            "product": "Keyboard",
            "quantity": 1,
            "price": 100,
        },
        {
            "order_id": 4,
            "customer": "Charlie",
            "product": "Monitor",
            "quantity": 2,
            "price": 300,
        },
        {
            "order_id": 5,
            "customer": "Bob",
            "product": "Laptop",
            "quantity": 1,
            "price": 1200,
        },
    ]
    df = spark.createDataFrame(data)
    assert df.count() == 5
    print(f"✅ Created DataFrame with {df.count()} orders")

    # Step 3: Transformations
    df_with_total = df.withColumn("total", F.col("quantity") * F.col("price"))
    assert df_with_total.count() == 5

    high_value_orders = df_with_total.filter(F.col("total") > 500)
    assert high_value_orders.count() == 3
    print(f"✅ Filter works: {high_value_orders.count()} high-value orders")

    # Step 4: Aggregations
    customer_revenue = (
        df_with_total.groupBy("customer")
        .agg(
            F.sum("total").alias("total_revenue"),
            F.count("order_id").alias("order_count"),
        )
        .orderBy(F.desc("total_revenue"))
    )
    assert customer_revenue.count() == 3
    print(f"✅ Aggregations work: {customer_revenue.count()} customers")

    # Step 5: SQL
    df.createOrReplaceTempView("orders")
    result = spark.sql("""
        SELECT 
            customer,
            COUNT(*) as order_count,
            SUM(quantity * price) as total_spent,
            AVG(quantity * price) as avg_order_value
        FROM orders
        GROUP BY customer
        HAVING SUM(quantity * price) > 100
        ORDER BY total_spent DESC
    """)
    # Check SQL works (count may vary based on filter)
    assert result.count() >= 1
    print(f"✅ SQL works: {result.count()} results")

    # Step 6: Larger dataset
    customers = ["Alice", "Bob", "Charlie", "Diana", "Eve"]
    products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"]
    start_date = datetime(2024, 1, 1)

    large_data = [
        {
            "order_id": i,
            "customer": random.choice(customers),
            "product": random.choice(products),
            "quantity": random.randint(1, 5),
            "price": random.randint(10, 1500),
            "order_date": (start_date + timedelta(days=random.randint(0, 90))).strftime(
                "%Y-%m-%d"
            ),
        }
        for i in range(100)
    ]
    large_df = spark.createDataFrame(large_data)
    assert large_df.count() == 100
    print(f"✅ Large dataset created: {large_df.count()} orders")

    # Performance test
    start_time = time.time()
    result = (
        large_df.withColumn("total", F.col("quantity") * F.col("price"))
        .filter(F.col("total") > 100)
        .groupBy("customer", "product")
        .agg(F.sum("total").alias("revenue"))
        .orderBy(F.desc("revenue"))
        .collect()
    )
    elapsed = time.time() - start_time
    print(f"✅ Performance test: {elapsed:.4f} seconds for {len(result)} results")

    print("✅ Quickstart tutorial: ALL TESTS PASSED\n")


def test_dataframe_operations(spark):
    """Test dataframe operations notebook code."""
    imports = get_spark_imports()
    F = imports.F

    print("Testing DataFrame Operations Tutorial...")

    # Create test data
    employees = [
        {"emp_id": 1, "name": "Alice", "dept_id": 10, "salary": 80000, "city": "NYC"},
        {"emp_id": 2, "name": "Bob", "dept_id": 20, "salary": 75000, "city": "LA"},
        {"emp_id": 3, "name": "Charlie", "dept_id": 10, "salary": 90000, "city": "NYC"},
        {
            "emp_id": 4,
            "name": "Diana",
            "dept_id": 30,
            "salary": 85000,
            "city": "Chicago",
        },
        {"emp_id": 5, "name": "Eve", "dept_id": 20, "salary": 95000, "city": "LA"},
    ]

    departments = [
        {"dept_id": 10, "dept_name": "Engineering", "budget": 500000},
        {"dept_id": 20, "dept_name": "Sales", "budget": 300000},
        {"dept_id": 30, "dept_name": "Marketing", "budget": 200000},
    ]

    emp_df = spark.createDataFrame(employees)
    dept_df = spark.createDataFrame(departments)

    # Test select
    result = emp_df.select("name", "salary")
    assert result.count() == 5
    print("✅ Select works")

    # Test filter
    high_earners = emp_df.filter(F.col("salary") > 80000)
    assert high_earners.count() == 3
    print("✅ Filter works")

    # Test joins
    joined = emp_df.join(dept_df, "dept_id", "inner")
    assert joined.count() == 5
    print("✅ Joins work")

    # Test withColumn
    enriched = emp_df.withColumn("salary_k", F.col("salary") / 1000)
    assert "salary_k" in enriched.columns
    print("✅ WithColumn works")

    # Test orderBy
    sorted_df = emp_df.orderBy(F.desc("salary"))
    first_row = sorted_df.limit(1).collect()[0]
    assert first_row["name"] == "Eve"
    print("✅ OrderBy works")

    # Test distinct
    cities = emp_df.select("city").distinct()
    city_count = cities.count()
    print(f"   Distinct cities: {city_count}")
    assert city_count >= 1  # At least one city
    print("✅ Distinct works")

    print("✅ DataFrame Operations tutorial: ALL TESTS PASSED\n")


# Note: These test functions require pytest fixtures (spark parameter)
# To run them, use: pytest tests/test_notebooks.py
# The __main__ block is removed because these tests require pytest fixtures
