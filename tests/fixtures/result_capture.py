"""
Result capture utilities for comparison testing.

Captures and stores results from both mock-spark and PySpark for comparison
and baseline generation.
"""

import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from datetime import datetime


class ResultCapture:
    """Capture and store test results for comparison."""

    def __init__(self, output_dir: Optional[Path] = None):
        """Initialize result capture.

        Args:
            output_dir: Directory to store captured results. Defaults to tests/comparison_results/.
        """
        if output_dir is None:
            output_dir = Path(__file__).parent.parent / "comparison_results"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def capture_dataframe_result(
        self,
        test_name: str,
        mock_result: Any,
        pyspark_result: Optional[Any] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Capture DataFrame result for comparison.

        Args:
            test_name: Name of the test.
            mock_result: mock-spark DataFrame result.
            pyspark_result: Optional PySpark DataFrame result.
            metadata: Optional metadata about the test.

        Returns:
            Dictionary with captured results.
        """
        result = {
            "test_name": test_name,
            "timestamp": datetime.now().isoformat(),
            "mock_result": self._serialize_dataframe(mock_result),
        }

        if pyspark_result is not None:
            result["pyspark_result"] = self._serialize_dataframe(pyspark_result)

        if metadata:
            result["metadata"] = metadata

        # Save to file
        output_file = self.output_dir / f"{test_name}.json"
        with open(output_file, "w") as f:
            json.dump(result, f, indent=2, default=str)

        return result

    def _serialize_dataframe(self, df: Any) -> Dict[str, Any]:
        """Serialize DataFrame to dictionary.

        Args:
            df: DataFrame to serialize.

        Returns:
            Dictionary representation of DataFrame.
        """
        try:
            # Get schema
            schema = df.schema
            schema_dict = {
                "fields": [
                    {
                        "name": field.name,
                        "type": field.dataType.__class__.__name__,
                        "nullable": getattr(field, "nullable", True),
                    }
                    for field in schema.fields
                ]
            }

            # Get data
            rows = df.collect()
            data = [self._serialize_row(row) for row in rows]

            return {
                "schema": schema_dict,
                "count": len(data),
                "data": data,
            }
        except Exception as e:
            return {"error": str(e)}

    def _serialize_row(self, row: Any) -> Dict[str, Any]:
        """Serialize a row to dictionary.

        Args:
            row: Row object.

        Returns:
            Dictionary representation of row.
        """
        if isinstance(row, dict):
            return row
        elif hasattr(row, "asDict"):
            as_dict_result = row.asDict()
            if isinstance(as_dict_result, dict):
                return as_dict_result
            return {}  # Fallback to empty dict if asDict doesn't return dict
        else:
            # Try to convert to dict
            result: Dict[str, Any] = {}
            if hasattr(row, "__dict__"):
                row_dict = row.__dict__
                if isinstance(row_dict, dict):
                    result = row_dict
            elif hasattr(row, "_fields"):
                # Row-like object
                for field in row._fields:
                    result[field] = getattr(row, field, None)
            return result

    def load_baseline(self, test_name: str) -> Optional[Dict[str, Any]]:
        """Load baseline result for a test.

        Args:
            test_name: Name of the test.

        Returns:
            Baseline result dictionary or None if not found.
        """
        baseline_file = self.output_dir / f"{test_name}.json"
        if not baseline_file.exists():
            return None

        with open(baseline_file) as f:
            return json.load(f)  # type: ignore[no-any-return]

    def compare_with_baseline(
        self, test_name: str, current_result: Any
    ) -> Tuple[bool, Optional[str]]:
        """Compare current result with baseline.

        Args:
            test_name: Name of the test.
            current_result: Current DataFrame result.

        Returns:
            Tuple of (is_equal, error_message).
        """
        baseline = self.load_baseline(test_name)
        if baseline is None:
            return False, "No baseline found"

        current_serialized = self._serialize_dataframe(current_result)
        baseline_serialized = baseline.get("pyspark_result") or baseline.get(
            "mock_result"
        )

        if baseline_serialized is None:
            return False, "No baseline serialized result found"

        if current_serialized.get("count") != baseline_serialized.get("count"):
            return False, "Row count mismatch"

        # Simple comparison - could be enhanced
        return True, None
