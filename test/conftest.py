"""
This conftest file contains a scope that is used to make fixtures 
accessible throughout the entire directory. Instead of importing 
them into every test file, we can declare our spark session here and use it throughout 
the projects directory. When we run the docker-compose up test command our test function will request
the spark_session fixture, and pytest will find it and capture what it returns. In our case when we 
we call spark_session, pytest will return the SparkSession defined in the function. 
"""
import findspark
findspark.init()

import pytest

from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session(request):
    spark = SparkSession.builder.master("local[*]").appName("TestTransformations")\
                        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
                        .getOrCreate()

    return spark