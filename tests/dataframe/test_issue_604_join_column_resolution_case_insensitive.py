"""
Tests for issue #604: join when key names differ in case (left "id", right "ID").

After an inner join on "id", collect must not fail with "not found: ID".
The result must allow resolving the key column case-insensitively (e.g. select("ID")
when the column is named "id"), matching the Rust join_column_resolution_case_insensitive test.
"""

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
SparkSession = _imports.SparkSession


def _row_keys(row):
    """Return keys of a row (backend-agnostic: Row or dict)."""
    if hasattr(row, "asDict"):
        return list(row.asDict().keys())
    if hasattr(row, "__getitem__") and hasattr(row, "keys"):
        return list(row.keys())
    return []


def _row_val(row, *names):
    """Get value from row by name (tries each name for backend-agnostic access)."""
    for name in names:
        try:
            if hasattr(row, "asDict"):
                d = row.asDict()
                if name in d:
                    return d[name]
            if hasattr(row, "__getitem__"):
                return row[name]
        except (KeyError, TypeError):
            continue
    return None


class TestIssue604JoinColumnResolutionCaseInsensitive:
    """Join with different case key names; collect and column resolution must succeed."""

    def test_join_on_id_id_inner_collect_and_resolve_id(self, spark):
        """Join on 'id' with left 'id' and right 'ID'; collect must not fail; resolve 'ID' must work."""
        # Left: lowercase "id"; right: uppercase "ID" (issue #604)
        df1 = spark.createDataFrame([{"id": 1, "val": "a"}, {"id": 2, "val": "b"}])
        df2 = spark.createDataFrame([{"ID": 1, "other": "x"}])

        out = df1.join(df2, on="id", how="inner")

        assert out.count() == 1

        rows = out.collect()
        assert len(rows) == 1
        row = rows[0]
        keys = _row_keys(row)
        assert "id" in keys or "ID" in keys
        assert "val" in keys
        assert "other" in keys

        # Same assertions as Rust join_column_resolution_case_insensitive: column access.
        assert _row_val(row, "id", "ID") == 1
        assert _row_val(row, "val") == "a"
        assert _row_val(row, "other") == "x"

        # Resolving "ID" (case-insensitive) must work (mirrors Rust resolve_column_name("ID")).
        out.select("ID").collect()
