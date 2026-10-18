import pytest
from streaming.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    s = get_spark("tests")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _read(spark, path):
    return spark.read.format("delta").load(f"s3a://lakehouse/{path}")


@pytest.fixture(scope="session")
def bronze(spark):
    # cache = one consistent snapshot for every test, even if Bronze is still streaming
    return _read(spark, "bronze/events").cache()


@pytest.fixture(scope="session")
def silver(spark):
    return _read(spark, "silver/events").cache()


@pytest.fixture(scope="session")
def quarantine(spark):
    return _read(spark, "quarantine/events").cache()


def pytest_collection_modifyitems(items):
    for item in items:
        if {"bronze", "silver", "quarantine"} & set(item.fixturenames):
            item.add_marker(pytest.mark.integration)