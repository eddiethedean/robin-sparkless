"""
Output loader for expected PySpark test results.

This module loads pre-generated expected outputs from JSON files
and provides utilities for caching and accessing them during tests.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, cast
from functools import lru_cache


class ExpectedOutputLoader:
    """Loads and caches expected outputs from JSON files."""

    def __init__(self, base_dir: Union[Path, str, None] = None):
        """Initialize the loader with the expected outputs directory."""
        if base_dir is None:
            base_path = Path(__file__).parent.parent / "expected_outputs"
        else:
            base_path = Path(base_dir)
        self.base_dir = base_path
        self._cache: Dict[str, Any] = {}

    @lru_cache(maxsize=128)
    def load_output(
        self, category: str, test_name: str, pyspark_version: str = "3.2"
    ) -> Dict[str, Any]:
        """
        Load expected output for a specific test.

        Args:
            category: Test category (e.g., 'dataframe_operations', 'functions')
            test_name: Name of the test file (without .json extension)
            pyspark_version: PySpark version to load (default: "3.2")

        Returns:
            Dictionary containing the expected output data

        Raises:
            FileNotFoundError: If the expected output file doesn't exist
            ValueError: If the JSON file is malformed or invalid schema
        """
        file_path = self.base_dir / category / f"{test_name}.json"

        if not file_path.exists():
            raise FileNotFoundError(f"Expected output file not found: {file_path}")

        try:
            with open(file_path) as f:
                data = json.load(f)

            # Validate expected output schema
            self._validate_output_schema(data, category, test_name)

            # Cache the loaded data
            cache_key = f"{category}/{test_name}/{pyspark_version}"
            self._cache[cache_key] = data

            return cast("Dict[str, Any]", data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in expected output file {file_path}: {e}")

    def _validate_output_schema(
        self, data: Dict[str, Any], category: str, test_name: str
    ):
        """Validate that the expected output has the required schema."""
        required_fields = [
            "test_id",
            "pyspark_version",
            "generated_at",
            "input_data",
            "expected_output",
        ]

        for field in required_fields:
            if field not in data:
                raise ValueError(
                    f"Missing required field '{field}' in {category}/{test_name}.json"
                )

        expected_output = data.get("expected_output", {})
        required_output_fields = ["schema", "data", "row_count"]

        for field in required_output_fields:
            if field not in expected_output:
                raise ValueError(
                    f"Missing required field 'expected_output.{field}' in {category}/{test_name}.json"
                )

        # Validate schema structure
        schema = expected_output.get("schema", {})
        required_schema_fields = ["field_count", "field_names", "field_types", "fields"]

        for field in required_schema_fields:
            if field not in schema:
                raise ValueError(
                    f"Missing required field 'expected_output.schema.{field}' in {category}/{test_name}.json"
                )

    def load_category(self, category: str) -> Dict[str, Dict[str, Any]]:
        """
        Load all expected outputs for a category.

        Args:
            category: Test category name

        Returns:
            Dictionary mapping test names to their expected outputs
        """
        category_dir = self.base_dir / category
        if not category_dir.exists():
            return {}

        results = {}
        for json_file in category_dir.glob("*.json"):
            test_name = json_file.stem
            try:
                results[test_name] = self.load_output(category, test_name)
            except (FileNotFoundError, ValueError) as e:
                print(f"Warning: Could not load {json_file}: {e}")
                continue

        return results

    def get_available_categories(self) -> List[str]:
        """Get list of available test categories."""
        if not self.base_dir.exists():
            return []

        return [d.name for d in self.base_dir.iterdir() if d.is_dir()]

    def get_available_tests(self, category: str) -> List[str]:
        """Get list of available tests in a category."""
        category_dir = self.base_dir / category
        if not category_dir.exists():
            return []

        return [f.stem for f in category_dir.glob("*.json")]

    def get_metadata(self) -> Dict[str, Any]:
        """Load metadata about expected outputs."""
        metadata_file = self.base_dir / "metadata.json"
        if not metadata_file.exists():
            return {}

        try:
            with open(metadata_file) as f:
                return cast("Dict[str, Any]", json.load(f))
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def clear_cache(self):
        """Clear the internal cache."""
        self._cache.clear()
        self.load_output.cache_clear()


# Global loader instance
_loader: Optional[ExpectedOutputLoader] = None


def get_loader() -> ExpectedOutputLoader:
    """Get the global expected output loader instance."""
    global _loader
    if _loader is None:
        _loader = ExpectedOutputLoader()
    return _loader


def load_expected_output(
    category: str, test_name: str, pyspark_version: str = "3.2"
) -> Dict[str, Any]:
    """Convenience function to load expected output."""
    return get_loader().load_output(category, test_name, pyspark_version)


def load_category_outputs(category: str) -> Dict[str, Dict[str, Any]]:
    """Convenience function to load all outputs for a category."""
    return get_loader().load_category(category)


def get_available_categories() -> List[str]:
    """Convenience function to get available categories."""
    return get_loader().get_available_categories()


def get_available_tests(category: str) -> List[str]:
    """Convenience function to get available tests in a category."""
    return get_loader().get_available_tests(category)
