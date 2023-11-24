import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session(request):
    print("START PYSPARK SESSION")
    spark_session = (
        SparkSession.builder.master("local[1]").appName("PySparkUnitTest").getOrCreate()
    )

    yield spark_session