from pyspark.sql import SparkSession


class BaseValidate:
    def __init__(self, spark: SparkSession, business_logic: dict) -> None:
        self.spark = spark
        self.business_logic = business_logic
