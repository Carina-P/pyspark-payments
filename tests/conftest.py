import pytest
import uuid
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("Test payments ETL")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        yield spark
    finally:
        spark.stop()
