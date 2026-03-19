import tempfile
import uuid

import pytest
from sparkless.testing import create_session


@pytest.mark.delta
@pytest.mark.xdist_group(name="delta_serial")
def test_issue_1524_delta_saveastable_empty_dataframe_creates_table_then_append_works():
    warehouse_dir = tempfile.mkdtemp(prefix="sparkless_warehouse_")
    try:
        spark = create_session(
            app_name="issue_1524_delta_saveastable_empty",
            enable_delta=True,
            **{"spark.sql.warehouse.dir": warehouse_dir},
        )

        schema = f"demo_{uuid.uuid4().hex[:8]}"
        table = f"{schema}.delta_empty_table"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        empty_df = spark.createDataFrame([], "id int, name string")

        empty_df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(table)
        assert spark.table(table).count() == 0

        df1 = spark.createDataFrame([(1, "a")], ["id", "name"])
        df1.write.format("delta").mode("append").saveAsTable(table)
        assert spark.table(table).count() == 1
    finally:
        try:
            spark.stop()
        except Exception:
            pass
