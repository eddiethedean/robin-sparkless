import pytest

from tests.fixtures.spark_backend import get_backend_type, BackendType

# call_function is mock-spark specific - skip if using PySpark
_backend = get_backend_type()
if _backend == BackendType.PYSPARK:
    pytestmark = pytest.mark.skip(reason="call_function is mock-spark specific")
else:
    from sparkless.sql import functions as F
    from sparkless.errors import PySparkTypeError, PySparkValueError


def test_call_function_invokes_registered_function(spark):
    df = spark.createDataFrame([{"name": "alice"}])

    result = df.select(F.call_function("upper", "name").alias("upper_name")).collect()

    assert result[0]["upper_name"] == "ALICE"


def test_call_function_propagates_type_error():
    with pytest.raises(PySparkTypeError):
        F.call_function("upper")


def test_call_function_missing_name():
    with pytest.raises(PySparkValueError):
        F.call_function("does_not_exist", "name")
