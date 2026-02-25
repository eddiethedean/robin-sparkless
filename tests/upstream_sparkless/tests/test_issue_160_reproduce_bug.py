"""
Test to reproduce issue #160: cannot resolve error when execution plan references dropped columns.

This test uses the exact reproduction code from the issue comment.
The bug occurs when:
1. A column is used in transformations (e.g., F.regexp_replace(F.col("impression_date"), ...))
2. That column is dropped via .select()
3. Materialization is triggered (count(), collect(), cache(), etc.)
4. During materialization, validation checks column references in expressions
5. It tries to validate the dropped column which no longer exists

The bug ONLY occurs with 150+ rows, NOT with 2 rows.
"""

from sparkless import SparkSession, functions as F
from datetime import datetime, timedelta


class TestIssue160ReproduceBug:
    """Test cases to reproduce issue #160 with 150+ rows."""

    def test_bug_reproduction_with_150_rows(self):
        """Reproduce the bug with 150 rows using exact code from issue comment."""
        spark = SparkSession.builder.appName("bug_reproduction").getOrCreate()

        # Create test data (150 rows - bug occurs with larger datasets)
        data = []
        for i in range(150):
            data.append(
                {
                    "impression_id": f"IMP-{i:08d}",
                    "campaign_id": f"CAMP-{i % 10:02d}",
                    "customer_id": f"CUST-{i % 40:04d}",
                    "impression_date": (
                        datetime.now() - timedelta(hours=i % 720)
                    ).isoformat(),
                    "channel": ["google", "facebook", "twitter", "email", "display"][
                        i % 5
                    ],
                    "ad_id": f"AD-{i % 20:03d}",
                    "cost_per_impression": round(0.01 + (i % 50) / 1000, 3),
                    "device_type": ["desktop", "mobile", "tablet"][i % 3],
                }
            )

        bronze_df = spark.createDataFrame(
            data,
            [
                "impression_id",
                "campaign_id",
                "customer_id",
                "impression_date",  # This column will be dropped
                "channel",
                "ad_id",
                "cost_per_impression",
                "device_type",
            ],
        )

        # Apply transform that uses impression_date then drops it
        silver_df = (
            bronze_df.withColumn(
                "impression_date_parsed",
                F.to_timestamp(
                    F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast(
                        "string"
                    ),
                    "yyyy-MM-dd'T'HH:mm:ss",
                ),
            )
            .withColumn("hour_of_day", F.hour(F.col("impression_date_parsed")))
            .withColumn("day_of_week", F.dayofweek(F.col("impression_date_parsed")))
            .withColumn(
                "is_mobile",
                F.when(F.col("device_type") == "mobile", True).otherwise(False),
            )
            .select(
                "impression_id",
                "campaign_id",
                "customer_id",
                "impression_date_parsed",  # New column
                "hour_of_day",
                "day_of_week",
                "channel",
                "ad_id",
                "cost_per_impression",
                "device_type",
                "is_mobile",
                # NOTE: impression_date is DROPPED - not in select list
            )
        )

        # Verify column was dropped
        assert "impression_date" not in silver_df.columns
        assert "impression_date_parsed" in silver_df.columns

        # These operations should now work (bug is fixed)
        # Before the fix, they would raise SparkColumnNotFoundError
        count = silver_df.count()
        assert count == 150

        rows = silver_df.collect()
        assert len(rows) == 150

        # Verify cache works
        cached = silver_df.cache()
        assert cached is not None

        spark.stop()

    def test_bug_does_not_occur_with_2_rows(self):
        """Verify the bug does NOT occur with 2 rows (as mentioned in issue)."""
        spark = SparkSession.builder.appName("bug_reproduction").getOrCreate()

        # Create test data (2 rows - bug does NOT occur with small datasets)
        data = [
            {
                "impression_id": "IMP-00000000",
                "campaign_id": "CAMP-00",
                "customer_id": "CUST-0000",
                "impression_date": datetime.now().isoformat(),
                "channel": "google",
                "ad_id": "AD-000",
                "cost_per_impression": 0.01,
                "device_type": "desktop",
            },
            {
                "impression_id": "IMP-00000001",
                "campaign_id": "CAMP-01",
                "customer_id": "CUST-0001",
                "impression_date": (datetime.now() - timedelta(hours=1)).isoformat(),
                "channel": "facebook",
                "ad_id": "AD-001",
                "cost_per_impression": 0.02,
                "device_type": "mobile",
            },
        ]

        bronze_df = spark.createDataFrame(
            data,
            [
                "impression_id",
                "campaign_id",
                "customer_id",
                "impression_date",  # This column will be dropped
                "channel",
                "ad_id",
                "cost_per_impression",
                "device_type",
            ],
        )

        # Apply transform that uses impression_date then drops it
        silver_df = (
            bronze_df.withColumn(
                "impression_date_parsed",
                F.to_timestamp(
                    F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast(
                        "string"
                    ),
                    "yyyy-MM-dd'T'HH:mm:ss",
                ),
            )
            .withColumn("hour_of_day", F.hour(F.col("impression_date_parsed")))
            .withColumn("day_of_week", F.dayofweek(F.col("impression_date_parsed")))
            .withColumn(
                "is_mobile",
                F.when(F.col("device_type") == "mobile", True).otherwise(False),
            )
            .select(
                "impression_id",
                "campaign_id",
                "customer_id",
                "impression_date_parsed",  # New column
                "hour_of_day",
                "day_of_week",
                "channel",
                "ad_id",
                "cost_per_impression",
                "device_type",
                "is_mobile",
                # impression_date is DROPPED - not in select list
            )
        )

        # Verify column was dropped
        assert "impression_date" not in silver_df.columns
        assert "impression_date_parsed" in silver_df.columns

        # With 2 rows, this should work fine (bug does NOT occur)
        count = silver_df.count()
        assert count == 2

        rows = silver_df.collect()
        assert len(rows) == 2

        spark.stop()
