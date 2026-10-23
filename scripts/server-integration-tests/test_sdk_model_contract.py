"""Contract fixtures for the pinned Databricks Statement Execution models.

These optional integration fixtures verify public SDK decoding only. They do not replace the
integrated emulator gate, which must inspect the response produced by its real
adapter rather than a fabricated ``result.text`` response.
"""

from __future__ import annotations

import importlib.metadata
import unittest

try:
    from databricks.sdk.service.sql import StatementResponse
except ImportError:  # The lightweight local server test environment is optional.
    StatementResponse = None  # type: ignore[assignment,misc]


class PinnedStatementSdkModelTests(unittest.TestCase):
    @unittest.skipIf(StatementResponse is None, "install requirements-connect-4.0.4.lock")
    def test_public_models_preserve_exact_json_array_values_and_ordered_schema(self) -> None:
        self.assertEqual(importlib.metadata.version("databricks-sdk"), "0.156.0")
        response = StatementResponse.from_dict(
            {
                "statement_id": "fixture",
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "format": "JSON_ARRAY",
                    "schema": {
                        "column_count": 3,
                        "columns": [
                            {"name": "x", "position": 0, "type_name": "LONG", "type_text": "BIGINT"},
                            {
                                "name": "x",
                                "position": 1,
                                "type_name": "DECIMAL",
                                "type_text": "DECIMAL(38,2)",
                                "type_precision": 38,
                                "type_scale": 2,
                            },
                            {"name": "nullable", "position": 2, "type_name": "STRING", "type_text": "STRING"},
                        ],
                    },
                },
                "result": {
                    "chunk_index": 0,
                    "row_offset": 0,
                    "row_count": 1,
                    "data_array": [
                        [
                            "9007199254740993",
                            "123456789012345678901234567890123456.78",
                            None,
                        ]
                    ],
                },
            }
        )

        self.assertEqual(response.result.data_array[0], [
            "9007199254740993",
            "123456789012345678901234567890123456.78",
            None,
        ])
        columns = response.manifest.schema.columns
        self.assertEqual([column.name for column in columns], ["x", "x", "nullable"])
        self.assertEqual([column.position for column in columns], [0, 1, 2])
        self.assertEqual(columns[0].type_name.value, "LONG")
        self.assertEqual((columns[1].type_precision, columns[1].type_scale), (38, 2))

    @unittest.skipIf(StatementResponse is None, "install requirements-connect-4.0.4.lock")
    def test_empty_typed_result_keeps_manifest_schema(self) -> None:
        response = StatementResponse.from_dict(
            {
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "format": "JSON_ARRAY",
                    "schema": {
                        "column_count": 1,
                        "columns": [
                            {"name": "n", "position": 0, "type_name": "INT", "type_text": "INT"}
                        ],
                    },
                },
                "result": {"chunk_index": 0, "row_offset": 0, "row_count": 0, "data_array": []},
            }
        )
        self.assertEqual(response.result.data_array, [])
        self.assertEqual(response.manifest.schema.column_count, 1)
        self.assertEqual(response.manifest.schema.columns[0].type_name.value, "INT")


if __name__ == "__main__":
    unittest.main()
