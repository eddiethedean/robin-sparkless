from __future__ import annotations

from typing import TYPE_CHECKING, NoReturn

if TYPE_CHECKING:
    from sparkless import DataFrame


class DataFrameNaFunctions:
    """PySpark-compatible DataFrameNaFunctions wrapper."""

    def __init__(self, df: DataFrame) -> None:
        self._df = df

    def fill(
        self,
        value: int | float | str | bool | None,
        subset: list[str] | None = None,
    ) -> DataFrame:
        return self._df.fillna(value, subset=subset)

    def drop(
        self,
        how: str = "any",
        thresh: int | None = None,
        subset: list[str] | None = None,
    ) -> DataFrame:
        return self._df.dropna(how=how, thresh=thresh, subset=subset)

    def replace(
        self,
        to_replace: str | list[str] | dict[str, str | int | float | bool | None],
        value: str | int | float | bool | None = None,
        subset: list[str] | None = None,
    ) -> NoReturn:
        raise NotImplementedError("DataFrameNaFunctions.replace is not yet implemented")
