import logging

import pytest
from pyspark.sql import SparkSession

from src.extract.master_data_sql import GetIntegratedData
from src.utils.logging_util import get_logger

# Because this file is not a Databricks notebook, you
# must create a Spark session. Databricks notebooks
# create a Spark session for you by default.


@pytest.fixture
def spark_session():
    """Get Spark Session for Local development."""
    return (
        SparkSession.builder.appName("spark-unit-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.python.worker.memory", "1g")
        .master("local")
        .getOrCreate()
    )


@pytest.fixture
def get_integrated_data_mock(mocker):
    mocker.patch(
        "src.extract.master_data_sql.GetIntegratedData.__init__", return_value=None
    )
    return GetIntegratedData()  # Mocked class


@pytest.fixture(autouse=True)
def _enable_log_propagation():
    logger = get_logger()
    logger.setLevel(logging.DEBUG)
    logger.propagate = True
