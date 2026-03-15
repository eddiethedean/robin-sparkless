from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Union

if TYPE_CHECKING:
    from sparkless import DataFrame


class DataFrameNaFunctions:
    """PySpark-compatible DataFrameNaFunctions wrapper."""

    def __init__(self, df: DataFrame) -> None:
        self._df = df

    def fill(
        self,
        value: Union[int, float, str, bool, None],
        subset: Optional[List[str]] = None,
    ) -> DataFrame:
        return self._df.fillna(value, subset=subset)

    def drop(
        self,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: Optional[List[str]] = None,
    ) -> DataFrame:
        return self._df.dropna(how=how, thresh=thresh, subset=subset)

    def replace(
        self,
        to_replace: Union[
            str, List[str], Dict[str, Union[str, int, float, bool, None]]
        ],
        value: Optional[Union[str, int, float, bool, None]] = None,
        subset: Optional[List[str]] = None,
    ) -> DataFrame:
        """Replace values in columns. PySpark na.replace(to_replace, value, subset)."""
        return self._df.replace(to_replace, value, subset=subset)
