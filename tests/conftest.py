import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("pytest_spark") \
        .getOrCreate()

    yield spark

    spark.stop()
