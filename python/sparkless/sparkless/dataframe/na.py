class DataFrameNaFunctions:
    """PySpark-compatible DataFrameNaFunctions wrapper."""

    def __init__(self, df):
        self._df = df

    def fill(self, value, subset=None):
        return self._df.fillna(value, subset=subset)

    def drop(self, how="any", thresh=None, subset=None):
        return self._df.dropna(how=how, thresh=thresh, subset=subset)

    def replace(self, to_replace, value=None, subset=None):
        raise NotImplementedError("DataFrameNaFunctions.replace is not yet implemented")
