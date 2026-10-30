import os
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return SparkSession.builder.getOrCreate()
    return (
        SparkSession.builder.master("local[1]")
        .appName("crypto-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )