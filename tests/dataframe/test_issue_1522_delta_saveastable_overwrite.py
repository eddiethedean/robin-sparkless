import tempfile
import uuid

import pytest

from sparkless.testing import create_session


@pytest.mark.delta
@pytest.mark.xdist_group(name="delta_serial")
def test_issue_1522_delta_saveastable_overwrite_replaces_table_contents():
    warehouse_dir = tempfile.mkdtemp(prefix="sparkless_warehouse_")
    try:
        spark = create_session(
            app_name="issue_1522_delta_saveastable_overwrite",
            enable_delta=True,
            **{"spark.sql.warehouse.dir": warehouse_dir},
        )

        schema = f"demo_{uuid.uuid4().hex[:8]}"
        table = f"{schema}.overwrite_repro"

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        df1 = spark.createDataFrame([(1, "a")], ["id", "v"])
        df2 = spark.createDataFrame([(2, "b")], ["id", "v"])

        # First overwrite-to-create should succeed (table absent).
        df1.write.format("delta").mode("overwrite").saveAsTable(table)

        # Second overwrite against an existing table should succeed and replace contents.
        df2.write.format("delta").mode("overwrite").saveAsTable(table)

        rows = spark.table(table).collect()
        assert len(rows) == 1
        assert rows[0]["id"] == 2
        assert rows[0]["v"] == "b"
    finally:
        try:
            spark.stop()
        except Exception:
            pass
