from datetime import datetime
from typing import TypedDict

from pyspark.sql import SparkSession


class ProcessLogConfig(TypedDict):
    spark: SparkSession
    run_month: str
    record: dict[str, int | datetime | str]
