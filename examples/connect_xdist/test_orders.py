"""Each worker uses its own session with the shared native server."""


def test_filter(spark):
    assert spark.range(10).filter("id > 5").count() == 4


def test_count(spark):
    assert spark.range(10).count() == 10
